package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.implicits.given
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectionTermination
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import cats.effect.IO
import doobie.Read
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import fs2.Stream

import java.time.Instant

/**
  * Persistent log of projection terminations. Repeated occurrences of the same logical termination collapse into one
  * row whose `occurrences` counter is bumped and whose `last_occurrence` timestamp is refreshed.
  */
trait ProjectionTerminalStore {

  /**
    * Records a successful termination (natural end-of-stream or passivation).
    */
  def recordCompletion(metadata: ProjectionMetadata, instant: Instant): IO[Unit]

  /**
    * Records a failure termination. Distinct error classes for the same projection produce distinct rows.
    */
  def recordFailure(metadata: ProjectionMetadata, throwable: Throwable, instant: Instant): IO[Unit]

  /**
    * Streams all terminations, most recent first.
    */
  def stream: Stream[IO, ProjectionTermination]
}

object ProjectionTerminalStore {

  private val NoErrorClass: String = ""

  def apply(xas: Transactors, config: QueryConfig): ProjectionTerminalStore = new ProjectionTerminalStore {

    override def recordCompletion(metadata: ProjectionMetadata, instant: Instant): IO[Unit] =
      upsert(metadata, eventType = "completed", errorClass = NoErrorClass, errorMessage = "", instant)

    override def recordFailure(metadata: ProjectionMetadata, throwable: Throwable, instant: Instant): IO[Unit] =
      upsert(
        metadata,
        eventType = "failed",
        errorClass = throwable.getClass.getName,
        errorMessage = Option(throwable.getMessage).getOrElse(""),
        instant
      )

    private def upsert(
        metadata: ProjectionMetadata,
        eventType: String,
        errorClass: String,
        errorMessage: String,
        instant: Instant
    ): IO[Unit] =
      sql"""INSERT INTO public.projection_terminations
           |  (module, name, project, resource_id, event_type, error_class, error_message,
           |   first_occurrence, last_occurrence, occurrences)
           |VALUES (${metadata.module}, ${metadata.name}, ${metadata.project}, ${metadata.resourceId},
           |        $eventType, $errorClass, $errorMessage, $instant, $instant, 1)
           |ON CONFLICT (name, event_type, error_class) DO UPDATE SET
           |  last_occurrence = EXCLUDED.last_occurrence,
           |  occurrences     = public.projection_terminations.occurrences + 1,
           |  error_message   = EXCLUDED.error_message,
           |  module          = EXCLUDED.module,
           |  project         = EXCLUDED.project,
           |  resource_id     = EXCLUDED.resource_id
           |""".stripMargin.update.run
        .transact(xas.write)
        .void

    override def stream: Stream[IO, ProjectionTermination] =
      sql"""SELECT module, name, project, resource_id, event_type, error_class, error_message,
           |       first_occurrence, last_occurrence, occurrences
           |FROM public.projection_terminations
           |ORDER BY last_occurrence DESC
           |""".stripMargin
        .query[ProjectionTermination]
        .streamWithChunkSize(config.batchSize)
        .transact(xas.read)
  }

  /** Reads a row into the right variant based on `event_type`. */
  private given Read[ProjectionTermination] = {
    type Row = (String, String, Option[ProjectRef], Option[Iri], String, String, String, Instant, Instant, Long)
    Read[Row].map {
      case (module, name, project, resourceId, "completed", _, _, first, last, occurrences)              =>
        ProjectionTermination.Completed(ProjectionMetadata(module, name, project, resourceId), first, last, occurrences)
      case (module, name, project, resourceId, "failed", errClass, errMessage, first, last, occurrences) =>
        ProjectionTermination
          .Failed(ProjectionMetadata(module, name, project, resourceId), errClass, errMessage, first, last, occurrences)
      case (_, name, _, _, other, _, _, _, _, _)                                                         =>
        throw new IllegalStateException(s"Unknown event_type '$other' for projection '$name'.")
    }
  }
}
