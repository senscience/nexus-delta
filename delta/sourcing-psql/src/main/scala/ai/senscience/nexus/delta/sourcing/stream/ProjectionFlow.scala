package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.{IO, Ref}
import cats.syntax.all.*

/**
  * The progress state of a running projection together with the pipe that advances it: for each batch it updates the
  * live progress, records metrics, persists progress and saves failed elems.
  */
trait ProjectionFlow {

  /** The progress the projection starts from (fetched from the store, or none for a transient projection). */
  def initialProgress: ProjectionProgress

  /** The live progress, updated as each batch is processed. */
  def currentProgress: Ref[IO, ProjectionProgress]

  /** Pipe that, per batch, updates [[currentProgress]], records metrics, persists progress and saves failed elems. */
  def savingPipe[A]: ElemPipe[A, Unit]
}

object ProjectionFlow {

  type SaveProjectionErrors = List[FailedElem] => IO[Unit]

  private def testOffset(elem: Elem[?], progress: ProjectionProgress): Boolean =
    elem.offset.value > progress.offset.value

  /** The number of elems newly processed/discarded/failed between two cumulative progress snapshots. */
  private def progressDelta(previous: ProjectionProgress, current: ProjectionProgress): ProjectionProgress =
    current.copy(
      processed = current.processed - previous.processed,
      discarded = current.discarded - previous.discarded,
      failed = current.failed - previous.failed
    )

  /** Pipe tracking progress: advances the cumulative progress per elem, persisting it and failed elems per batch. */
  def persist[A](
      progress: ProjectionProgress,
      saveProgress: ProjectionProgress => IO[Unit],
      saveFailedElems: SaveProjectionErrors
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
          saveProgress(newProgress) >> IO.whenA(errors.nonEmpty)(saveFailedElems(errors))
        }
      }
      .drain

  private def make(
      initial: ProjectionProgress,
      metadata: ProjectionMetadata,
      saveProgress: ProjectionProgress => IO[Unit],
      saveFailedElems: SaveProjectionErrors,
      metrics: ProjectionMetrics
  )(using BatchConfig): IO[ProjectionFlow] =
    Ref[IO].of(initial).map { progressRef =>
      new ProjectionFlow {
        override def initialProgress: ProjectionProgress          = initial
        override def currentProgress: Ref[IO, ProjectionProgress] = progressRef
        override def savingPipe[A]: ElemPipe[A, Unit]             =
          persist(
            initial,
            newProgress =>
              progressRef.getAndSet(newProgress).flatMap { previous =>
                metrics.recordProgress(metadata, progressDelta(previous, newProgress))
              } >> saveProgress(newProgress),
            saveFailedElems
          )
      }
    }

  /** A flow that does not persist progress (metrics and failed elems are still reported). For transient projections. */
  def transient(
      metadata: ProjectionMetadata,
      saveFailedElems: SaveProjectionErrors,
      metrics: ProjectionMetrics
  )(using BatchConfig): IO[ProjectionFlow] =
    make(ProjectionProgress.NoProgress, metadata, _ => IO.unit, saveFailedElems, metrics)

  /** Builds the [[ProjectionFlow]] for a projection from the projection/error stores and metrics. */
  final class Builder(projections: Projections, projectionErrors: ProjectionErrors, metrics: ProjectionMetrics)(using
      BatchConfig
  ) {

    /** A flow that fetches and persists progress (for [[ExecutionStrategy.PersistentSingleNode]]). */
    def persisted(metadata: ProjectionMetadata): IO[ProjectionFlow] =
      projections.progress(metadata.name).flatMap { progress =>
        make(
          progress.getOrElse(ProjectionProgress.NoProgress),
          metadata,
          projections.save(metadata, _),
          projectionErrors.saveFailedElems(metadata, _),
          metrics
        )
      }

    /** A flow that does not persist progress (for transient / every-node projections). */
    def transient(metadata: ProjectionMetadata): IO[ProjectionFlow] =
      ProjectionFlow.transient(metadata, projectionErrors.saveFailedElems(metadata, _), metrics)
  }
}
