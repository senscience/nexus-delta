package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import cats.effect.{IO, Resource}
import fs2.Stream
import fs2.concurrent.SignallingRef

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

  final private class Active(supervisor: Supervisor, checkInterval: FiniteDuration, signal: SignallingRef[IO, Boolean])
      extends SupervisorCheck {
    override def run: IO[Unit] =
      Stream
        .awakeEvery[IO](checkInterval)
        .evalTap(_ => logger.debug("Checking projection statuses"))
        .flatMap(_ => supervisor.check)
        .interruptWhen(signal)
        .compile
        .drain

    override def stop: IO[Unit] = signal.set(true)
  }

  def apply(supervisor: Supervisor, checkInterval: FiniteDuration): Resource[IO, SupervisorCheck] = {
    Resource
      .make(
        logger.info("Starting supervisor check task") >>
          SignallingRef[IO, Boolean](false)
            .map { signal =>
              new Active(supervisor, checkInterval, signal)
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
