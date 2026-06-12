package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.Projection.logger
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.*
import cats.syntax.all.*
import fs2.concurrent.SignallingRef

import scala.concurrent.duration.FiniteDuration

/**
  * A reference to a projection that has been started.
  *
  * @param name
  *   the name of the projection
  * @param status
  *   the projection execution status; transitioning to [[ExecutionStatus.Stopped]] interrupts the underlying stream
  * @param progress
  *   the current projection progress
  * @param fiber
  *   the projection fiber
  */
final class Projection private[stream] (
    val name: String,
    status: SignallingRef[IO, ExecutionStatus],
    progress: Ref[IO, ProjectionProgress],
    fiber: Fiber[IO, Throwable, Unit]
) {

  /**
    * @return
    *   the current execution status of this projection
    */
  def executionStatus: IO[ExecutionStatus] = status.get

  /**
    * Return the current progress for this projection
    */
  def currentProgress: IO[ProjectionProgress] = progress.get

  /**
    * Wait for the projection to complete within the defined timeout
    * @param timeout
    *   the maximum time expected for the projection to complete
    */

  def waitForCompletion(timeout: FiniteDuration): IO[ExecutionStatus] =
    status
      .waitUntil(_.isTerminal)
      .timeoutTo(timeout, logger.error(s"Timeout waiting for completion on projection $name")) >> executionStatus

  /**
    * Stops the projection. Has no effect if the projection is already stopped.
    */
  def stop: IO[Unit] =
    for {
      _ <- status.set(ExecutionStatus.Stopped)
      _ <- fiber.join
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
        val errors = chunk.toList.flatMap(_._2)
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
      saveFailedElems: List[FailedElem] => IO[Unit],
      listener: ProjectionOutcomeListener
  )(using batch: BatchConfig): Resource[IO, Projection] =
    Resource.make(
      for {
        status      <- SignallingRef[IO, ExecutionStatus](ExecutionStatus.Pending)
        progress    <- fetchProgress.map(_.getOrElse(ProjectionProgress.NoProgress))
        progressRef <- Ref[IO].of(progress)
        stream       = projection.streamF.apply(progress.offset).interruptWhen(status.map(_.isStopped))
        persisted    =
          stream
            .through(
              persist(
                progress,
                (newProgress: ProjectionProgress) => progressRef.set(newProgress) >> saveProgress(newProgress),
                saveFailedElems
              )
            )
            .compile
            .drain
        task         = (status.set(ExecutionStatus.Running) >> persisted).guaranteeCase {
                         case Outcome.Succeeded(_) =>
                           status.update(s => if s.isRunning then ExecutionStatus.Completed else s) >>
                             progressRef.get.flatMap { finalProgress =>
                               val didWork = finalProgress.offset.value > progress.offset.value
                               listener.onCompletion(projection.metadata, didWork)
                             }
                         case Outcome.Errored(th)  =>
                           status.update(_.failed(th)) >> listener.onFailure(projection.metadata, th)
                         case Outcome.Canceled()   =>
                           IO.unit // status set by whoever cancelled
                       }
        fiber       <- task.start
      } yield new Projection(projection.metadata.name, status, progressRef, fiber)
    )(_.stop)

  /**
    * Creates a transient [[Projection]]: no progress is fetched or persisted, and listener notifications are
    * suppressed. Intended for tests and short-lived in-memory streams.
    *
    * @param compiled
    *   the compiled projection to run
    * @param saveFailedElems
    *   sink for failed elements (defaults to a no-op)
    */
  def transient(
      compiled: CompiledProjection,
      saveFailedElems: List[FailedElem] => IO[Unit] = _ => IO.unit
  )(using BatchConfig): Resource[IO, Projection] =
    apply(compiled, IO.none, _ => IO.unit, saveFailedElems, ProjectionOutcomeListener.noop)

}
