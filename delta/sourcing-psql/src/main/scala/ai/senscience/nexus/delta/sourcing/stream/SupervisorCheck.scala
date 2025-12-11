package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.sourcing.stream.Supervisor.{createRetryStrategy, restartProjection}
import ai.senscience.nexus.delta.sourcing.stream.SupervisorCheck.logger
import ai.senscience.nexus.delta.sourcing.stream.config.ProjectionConfig
import cats.effect.{Deferred, IO, Resource}

final class SupervisorCheck(supervisorStorage: SupervisorStorage, cfg: ProjectionConfig, halt: Deferred[IO, Unit]) {
  def run: IO[Unit] =
    supervisorStorage.failingStream
      .evalMap { projectionName =>
        supervisorStorage.update(projectionName)(heal).void
      }
      .interruptWhen(halt.get.attempt)
      .compile
      .drain

  private def heal(supervised: Supervised) = {
    val metadata = supervised.metadata
    supervised.control.status.flatMap {
      case ExecutionStatus.Failed(throwable) =>
        val retryStrategy = createRetryStrategy(cfg, metadata, "running")
        logger.error(throwable)(s"The projection '${metadata.fullName}' failed and will be restarted.") >>
          restartProjection(supervised)
            .retry(retryStrategy)
      case status                            =>
        logger
          .warn(s"The projection '${metadata.fullName}' was flagged as failed but it was $status")
          .as(supervised)
    }
  }

  def stop: IO[Unit] = halt.complete(()).void
}

object SupervisorCheck {

  private val logger = Logger[SupervisorCheck]

  def apply(supervisorStorage: SupervisorStorage, cfg: ProjectionConfig): Resource[IO, SupervisorCheck] = {
    Resource
      .make(
        logger.info("Starting supervisor check task") >>
          Deferred[IO, Unit]
            .map { halt =>
              new SupervisorCheck(supervisorStorage, cfg, halt)
            }
            .flatMap { s =>
              s.run.start.map(s -> _)
            }
      ) { case (s, fiber) =>
        s.stop >> fiber.join.void
      }
      .map(_._1)
  }

}
