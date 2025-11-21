package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.stream.config.BackpressureConfig
import cats.effect.IO
import cats.effect.std.Semaphore

trait ProjectionBackpressure {

  def acquire(metadata: ProjectionMetadata): IO[Unit]

  def release(metadata: ProjectionMetadata): IO[Unit]

}

object ProjectionBackpressure {

  private val logger = Logger[ProjectionBackpressure]

  object Noop extends ProjectionBackpressure {

    override def acquire(metadata: ProjectionMetadata): IO[Unit] = IO.unit

    override def release(metadata: ProjectionMetadata): IO[Unit] = IO.unit
  }

  def apply(config: BackpressureConfig): IO[ProjectionBackpressure] = {
    if config.enabled then
      Semaphore[IO](config.bound).map { semaphore =>
        new ProjectionBackpressure {
          override def acquire(metadata: ProjectionMetadata): IO[Unit] =
            logger.debug(s"Acquiring for '${metadata.fullName}'") >>
              semaphore.acquire

          override def release(metadata: ProjectionMetadata): IO[Unit] =
            logger.debug(s"Releasing for '${metadata.fullName}'") >>
              semaphore.release
        }
      }
    else IO.pure(Noop)
  }

}
