package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import ai.senscience.nexus.delta.sourcing.stream.Projection.logger
import ai.senscience.nexus.delta.sourcing.stream.ProjectionFlow.SaveProjectionErrors
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
    val status: SignallingRef[IO, ExecutionStatus],
    progress: Ref[IO, ProjectionProgress],
    fiber: Fiber[IO, Throwable, Unit]
) {

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
      .timeoutTo(timeout, logger.error(s"Timeout waiting for completion on projection $name")) >> status.get

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

  def apply(
      projection: CompiledProjection,
      flow: IO[ProjectionFlow],
      listener: ProjectionOutcomeListener
  ): Resource[IO, Projection] =
    Resource.make(
      for {
        projectionFlow <- flow
        status         <- SignallingRef[IO, ExecutionStatus](ExecutionStatus.Pending)
        progress        = projectionFlow.initialProgress
        progressRef     = projectionFlow.currentProgress
        stream          = projection.streamF.apply(progress.offset).interruptWhen(status.map(_.isStopped))
        persisted       = stream.through(projectionFlow.savingPipe).compile.drain
        task            = (status.set(ExecutionStatus.Running) >> persisted).guaranteeCase {
                            case Outcome.Succeeded(_) =>
                              status.update(s => if s.isRunning then ExecutionStatus.Completed else s) >>
                                progressRef.get.flatMap { finalProgress =>
                                  val didWork = finalProgress.offset > progress.offset
                                  listener.onCompletion(projection.metadata, didWork)
                                }
                            case Outcome.Errored(th)  =>
                              status.update(_.failed(th)) >> listener.onFailure(projection.metadata, th)
                            case Outcome.Canceled()   =>
                              IO.unit // status set by whoever cancelled
                          }
        fiber          <- task.start
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
      saveFailedElems: SaveProjectionErrors = _ => IO.unit
  )(using BatchConfig): Resource[IO, Projection] =
    apply(
      compiled,
      ProjectionFlow.transient(compiled.metadata, saveFailedElems, ProjectionMetrics.Disabled),
      ProjectionOutcomeListener.noop
    )

}
