package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.{FailedElemLogRow, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.{FailureReason, ProjectionMetadata, ProjectionStore}
import ai.senscience.nexus.delta.sourcing.{FragmentEncoder, Transactors}
import cats.effect.{Clock, IO}
import cats.implicits.*
import doobie.*
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import fs2.Stream

import java.time.Instant

/**
  * Persistent operations for errors raised by projections
  */
trait FailedElemLogStore {

  /**
    * Returns the total number of elems
    */
  def count: IO[Long]

  /**
    * Saves a list of failed elems
    *
    * @param metadata
    *   the metadata of the projection
    * @param failures
    *   the FailedElem to save
    */
  def save(metadata: ProjectionMetadata, failures: List[FailedElem]): IO[Unit]

  /**
    * Saves one failed elem
    */
  protected def saveFailedElem(metadata: ProjectionMetadata, failure: FailedElem, instant: Instant): ConnectionIO[Unit]

  /**
    * Get available failed elem entries for a given projection (provided by project and id), starting from a failed elem
    * offset.
    *
    * @param projectionProject
    *   the project the projection belongs to
    * @param projectionId
    *   IRI of the projection
    * @param offset
    *   failed elem offset
    */
  def stream(
      projectionProject: ProjectRef,
      projectionId: Iri,
      offset: Offset
  ): Stream[IO, FailedElemLogRow]

  /**
    * Get available failed elem entries for a given projection by projection name, starting from a failed elem offset.
    *
    * @param projectionName
    *   the name of the projection
    * @param offset
    *   failed elem offset
    * @return
    */
  def stream(
      projectionName: String,
      offset: Offset
  ): Stream[IO, FailedElemLogRow]

  /**
    * Return a list of errors for the given projection on a time window ordered by instant
    *
    * @param project
    *   the project of the projection
    * @param projectionId
    *   its identifier
    * @param timeRange
    *   the time range to restrict on
    * @return
    */
  def count(project: ProjectRef, projectionId: Iri, timeRange: TimeRange): IO[Long]

  /**
    * Return a list of errors for the given projection on a time window ordered by instant
    * @param project
    *   the project of the projection
    * @param projectionId
    *   its identifier
    * @param pagination
    *   the pagination to apply
    * @param timeRange
    *   the time range to restrict on
    * @return
    */
  def list(
      project: ProjectRef,
      projectionId: Iri,
      pagination: FromPagination,
      timeRange: TimeRange
  ): IO[List[FailedElemLogRow]]

  /**
    * Delete the errors related to the given projection
    * @param projectionName
    *   the projection name
    */
  def deleteEntriesForProjection(projectionName: String): IO[Unit]

}

object FailedElemLogStore {

  private val logger = Logger[ProjectionStore]

  def apply(xas: Transactors, config: QueryConfig, clock: Clock[IO]): FailedElemLogStore =
    new FailedElemLogStore {

      implicit val timeRangeFragmentEncoder: FragmentEncoder[TimeRange] = createTimeRangeFragmentEncoder("instant")

      override def count: IO[Long] =
        sql"SELECT count(ordering) FROM public.failed_elem_logs"
          .query[Long]
          .unique
          .transact(xas.read)

      override def save(metadata: ProjectionMetadata, failures: List[FailedElem]): IO[Unit] = {
        val log  = logger.debug(s"[${metadata.name}] Saving ${failures.length} failed elems.")
        val save = clock.realTimeInstant.flatMap { instant =>
          failures.traverse(elem => saveFailedElem(metadata, elem, instant)).transact(xas.write).void
        }
        log >> save
      }

      override protected def saveFailedElem(
          metadata: ProjectionMetadata,
          failure: FailedElem,
          instant: Instant
      ): ConnectionIO[Unit] = {
        val failureReason = failure.throwable match {
          case f: FailureReason => f
          case t                => FailureReason(t)
        }
        sql"""
           | INSERT INTO public.failed_elem_logs (
           |  projection_name,
           |  projection_module,
           |  projection_project,
           |  projection_id,
           |  entity_type,
           |  elem_offset,
           |  elem_id,
           |  elem_project,
           |  rev,
           |  error_type,
           |  reason,
           |  instant
           | )
           | VALUES (
           |  ${metadata.name},
           |  ${metadata.module},
           |  ${metadata.project},
           |  ${metadata.resourceId},
           |  ${failure.tpe},
           |  ${failure.offset},
           |  ${failure.id},
           |  ${failure.project},
           |  ${failure.rev},
           |  ${failureReason.`type`},
           |  ${failureReason.value},
           |  $instant
           | )""".stripMargin.update.run.void
      }

      override def stream(
          projectionProject: ProjectRef,
          projectionId: Iri,
          offset: Offset
      ): Stream[IO, FailedElemLogRow] =
        sql"""SELECT * from public.failed_elem_logs
           |WHERE projection_project = $projectionProject
           |AND projection_id = $projectionId
           |AND ordering > $offset
           |ORDER BY ordering ASC""".stripMargin
          .query[FailedElemLogRow]
          .streamWithChunkSize(config.batchSize)
          .transact(xas.read)

      override def stream(projectionName: String, offset: Offset): Stream[IO, FailedElemLogRow] =
        sql"""SELECT * from public.failed_elem_logs
           |WHERE projection_name = $projectionName
           |AND ordering > $offset
           |ORDER BY ordering ASC""".stripMargin
          .query[FailedElemLogRow]
          .streamWithChunkSize(config.batchSize)
          .transact(xas.read)

      override def count(project: ProjectRef, projectionId: Iri, timeRange: TimeRange): IO[Long] =
        sql"SELECT count(ordering) from public.failed_elem_logs  ${whereClause(project, projectionId, timeRange)}"
          .query[Long]
          .unique
          .transact(xas.read)

      override def list(
          project: ProjectRef,
          projectionId: Iri,
          pagination: FromPagination,
          timeRange: TimeRange
      ): IO[List[FailedElemLogRow]] =
        sql"""SELECT * from public.failed_elem_logs
             |${whereClause(project, projectionId, timeRange)}
             |ORDER BY ordering ASC
             |LIMIT ${pagination.size} OFFSET ${pagination.from}""".stripMargin
          .query[FailedElemLogRow]
          .to[List]
          .transact(xas.read)

      private def whereClause(project: ProjectRef, projectionId: Iri, timeRange: TimeRange) = Fragments.whereAndOpt(
        Some(fr"projection_project = $project"),
        Some(fr"projection_id = $projectionId"),
        timeRange.asFragment
      )

      override def deleteEntriesForProjection(projectionName: String): IO[Unit] =
        sql"""DELETE FROM public.failed_elem_logs WHERE projection_name = $projectionName""".stripMargin.update.run
          .transact(xas.write)
          .flatMap { deleted =>
            IO.whenA(deleted > 0)(logger.info(s"Deleted $deleted projection failures for '$projectionName'."))
          }
    }

}
