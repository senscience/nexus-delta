package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.stream.config.BackpressureConfig
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Semaphore
import cats.syntax.all.*

trait ProjectionBackpressure {

  def acquire(metadata: ProjectionMetadata, chunkSize: Int): IO[Unit]

  def release(metadata: ProjectionMetadata, chunkSize: Int): IO[Unit]

  def exhausted: IO[Boolean]

}

object ProjectionBackpressure {

  private val logger = Logger[ProjectionBackpressure]

  object Noop extends ProjectionBackpressure {

    override def acquire(metadata: ProjectionMetadata, chunkSize: Int): IO[Unit] = IO.unit

    override def release(metadata: ProjectionMetadata, chunkSize: Int): IO[Unit] = IO.unit

    override def exhausted: IO[Boolean] = IO.pure(false)
  }

  def apply(config: BackpressureConfig): IO[ProjectionBackpressure] = {
    if config.enabled then
      (Semaphore[IO](config.bound), Ref.of[IO, Int](0)).mapN { (semaphore, elemCount) =>
        new ProjectionBackpressure {
          override def acquire(metadata: ProjectionMetadata, chunkSize: Int): IO[Unit] =
            logger.trace(s"Acquiring for '${metadata.fullName}'") >>
              elemCount.update(_ + chunkSize) >>
                semaphore.acquire

          override def release(metadata: ProjectionMetadata, chunkSize: Int): IO[Unit] =
            logger.trace(s"Releasing for '${metadata.fullName}'") >>
              elemCount.update(_ - chunkSize) >>
              semaphore.release

          override def exhausted: IO[Boolean] = elemCount.get.map(_ > config.maxElems)
        }
      }
    else IO.pure(Noop)
  }

}
