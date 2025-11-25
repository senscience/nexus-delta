package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.Projection.logger
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.*
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import fs2.concurrent.SignallingRef

import scala.concurrent.duration.FiniteDuration

/**
  * A reference to a projection that has been started.
  *
  * @param name
  *   the name of the projection
  * @param status
  *   the projection execution status
  * @param signal
  *   a signal to stop the projection
  * @param fiber
  *   the projection fiber
  */
final class Projection private[stream] (
    val name: String,
    status: SignallingRef[IO, ExecutionStatus],
    progress: Ref[IO, ProjectionProgress],
    signal: SignallingRef[IO, Boolean],
    fiber: Ref[IO, Fiber[IO, Throwable, Unit]]
) {

  /**
    * @return
    *   the current execution status of this projection
    */
  def executionStatus: IO[ExecutionStatus] =
    status.get

  /**
    * Return the current progress for this projection
    * @return
    */
  def currentProgress: IO[ProjectionProgress] = progress.get

  /**
    * Wait for the projection to complete within the defined timeout
    * @param timeout
    *   the maximum time expected for the projection to complete
    * @return
    */

  def waitForCompletion(timeout: FiniteDuration): IO[ExecutionStatus] =
    status
      .waitUntil(statusMeansStopped)
      .timeoutTo(timeout, logger.error(s"Timeout waiting for completion on projection $name")) >> executionStatus

  private def statusMeansStopped(executionStatus: ExecutionStatus): Boolean = {
    executionStatus match {
      case ExecutionStatus.Completed => true
      case ExecutionStatus.Failed(_) => true
      case ExecutionStatus.Stopped   => true
      case _                         => false
    }
  }

  /**
    * Stops the projection. Has no effect if the projection is already stopped.
    */
  def stop(): IO[Unit] =
    for {
      f <- fiber.get
      _ <- status.update(_ => ExecutionStatus.Stopped)
      _ <- signal.set(true)
      _ <- f.join
    } yield ()
}

object Projection {

  private val logger = Logger[Projection]

  private def testOffset(elem: Elem[?], progress: ProjectionProgress) = elem.offset.value > progress.offset.value

  def persist[A](
      progress: ProjectionProgress,
      saveProgress: ProjectionProgress => IO[Unit],
      saveFailedElems: List[FailedElem] => IO[Unit]
  )(using batch: BatchConfig): ElemPipe[A, Unit] =
    _.mapAccumulate(progress) {
      case (acc, failed: FailedElem) if testOffset(failed, progress) => (acc + failed, Some(failed))
      case (acc, failed: FailedElem)                                 => (acc, Some(failed))
      case (acc, elem) if testOffset(elem, progress)                 => (acc + elem, None)
      case (acc, _)                                                  => (acc, None)
    }.groupWithin(batch.maxElements, batch.maxInterval)
      .evalTap { chunk =>
        val errors = chunk.foldLeft(List.empty[FailedElem]) { case (acc, (_, failed)) =>
          acc ++ failed
        }

        chunk.last.traverse { case (newProgress, _) =>
          saveProgress(newProgress) >>
            IO.whenA(errors.nonEmpty)(saveFailedElems(errors))
        }
      }
      .drain

  def apply(
      projection: CompiledProjection,
      fetchProgress: IO[Option[ProjectionProgress]],
      saveProgress: ProjectionProgress => IO[Unit],
      saveFailedElems: List[FailedElem] => IO[Unit]
  )(using batch: BatchConfig): IO[Projection] =
    apply(projection, IO.unit, fetchProgress, saveProgress, saveFailedElems)

  def apply(
      projection: CompiledProjection,
      init: IO[Unit],
      fetchProgress: IO[Option[ProjectionProgress]],
      saveProgress: ProjectionProgress => IO[Unit],
      saveFailedElems: List[FailedElem] => IO[Unit]
  )(using batch: BatchConfig): IO[Projection] =
    for {
      status      <- SignallingRef[IO, ExecutionStatus](ExecutionStatus.Pending)
      signal      <- SignallingRef[IO, Boolean](false)
      progress    <- fetchProgress.map(_.getOrElse(ProjectionProgress.NoProgress))
      progressRef <- Ref[IO].of(progress)
      stream       = projection.streamF
                       .apply(progress.offset)(status)(signal)
                       .interruptWhen(signal)
                       .onFinalizeCaseWeak {
                         case ExitCase.Errored(th) => status.update(_.failed(th))
                         case ExitCase.Succeeded   => IO.unit // streams stopped through a signal still finish as Completed
                         case ExitCase.Canceled    => IO.unit // the status is updated by the logic that cancels the stream
                       }
      persisted    =
        stream
          .through(
            persist(
              progress,
              (progress: ProjectionProgress) => progressRef.set(progress) >> saveProgress(progress),
              saveFailedElems
            )
          )
          .compile
          .drain
      // update status to Running at the beginning and to Completed at the end if it's still running
      task         = init >> status.update(_ => ExecutionStatus.Running) >> persisted >> status.update(s =>
                       if s.isRunning then ExecutionStatus.Completed else s
                     )
      fiber       <- task.start
      fiberRef    <- Ref[IO].of(fiber)
    } yield new Projection(projection.metadata.name, status, progressRef, signal, fiberRef)

}
