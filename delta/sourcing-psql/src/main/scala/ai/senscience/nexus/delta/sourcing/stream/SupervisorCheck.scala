package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import cats.effect.{Deferred, IO, Resource}
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

trait SupervisorCheck {
  def run: IO[Unit]

  def stop: IO[Unit]
}

object SupervisorCheck {

  private val logger = Logger[SupervisorCheck]

  object Disabled extends SupervisorCheck {
    override def run: IO[Unit] = IO.unit

    override def stop: IO[Unit] = IO.unit
  }

  final private class Active(supervisor: Supervisor, checkInterval: FiniteDuration, halt: Deferred[IO, Unit])
      extends SupervisorCheck {
    override def run: IO[Unit] =
      Stream
        .awakeEvery[IO](checkInterval)
        .evalTap(_ => logger.debug("Checking projection statuses"))
        .flatMap(_ => supervisor.check)
        .interruptWhen(halt.get.attempt)
        .compile
        .drain

    override def stop: IO[Unit] = halt.complete(()).void
  }

  def apply(supervisor: Supervisor, checkInterval: FiniteDuration): Resource[IO, SupervisorCheck] = {
    Resource
      .make(
        logger.info("Starting supervisor check task") >>
          Deferred[IO, Unit]
            .map { halt =>
              new Active(supervisor, checkInterval, halt)
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
