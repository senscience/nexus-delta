package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.implicits.{given, *}
import ai.senscience.nexus.delta.sourcing.model.FailedElemLog
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.{FailureReason, ProjectionMetadata}
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
    * Returns the total number of elems in the given time range
    */
  def count(timeRange: TimeRange): IO[Long]

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
    * Get available failed elem entries for a given projection (provided by project and id), starting from a failed elem
    * offset.
    *
    * @param selector
    *   to selector the projection by name or id
    * @param offset
    *   failed elem offset
    */
  def stream(selector: ProjectionSelector, offset: Offset): Stream[IO, FailedElemLog]

  /**
    * Return a list of errors for the given projection on a time window ordered by instant
    *
    * @param selector
    *   to select the projection by name or id
    * @param timeRange
    *   the time range to restrict on
    * @return
    */
  def count(selector: ProjectionSelector, timeRange: TimeRange): IO[Long]

  /**
    * Return a list of errors for the given projection on a time window ordered by instant
    * @param selector
    *   to select the projection by name or id
    * @param pagination
    *   the pagination to apply
    * @param timeRange
    *   the time range to restrict on
    * @return
    */
  def list(
      selector: ProjectionSelector,
      pagination: FromPagination,
      timeRange: TimeRange
  ): IO[List[FailedElemLog]]

  /**
    * Return all persisted errors
    * @param size
    *   the number of errors to return
    */
  def latest(size: Int): IO[List[FailedElemLog]]

  /**
    * Delete the errors related to the given projection
    * @param projectionName
    *   the projection name
    */
  def deleteEntriesForProjection(projectionName: String): IO[Unit]

}

object FailedElemLogStore {

  private val logger = Logger[FailedElemLogStore]

  def apply(xas: Transactors, config: QueryConfig, clock: Clock[IO]): FailedElemLogStore =
    new FailedElemLogStore {

      private given timeRangeFragmentEncoder: FragmentEncoder[TimeRange] = createTimeRangeFragmentEncoder("instant")

      override def count(timeRange: TimeRange): IO[Long] = {
        val whereInstant = Fragments.whereAndOpt(timeRange.asFragment)
        sql"SELECT count(ordering) FROM public.failed_elem_logs $whereInstant"
          .query[Long]
          .unique
          .transact(xas.read)
      }

      override def save(metadata: ProjectionMetadata, failures: List[FailedElem]): IO[Unit] =
        for {
          _   <- logger.debug(s"[${metadata.name}] Saving ${failures.length} failed elems.")
          now <- clock.realTimeInstant
          _   <- failures.traverse(elem => saveFailedElem(metadata, elem, now)).transact(xas.write)
        } yield ()

      private def saveFailedElem(
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

      override def stream(selector: ProjectionSelector, offset: Offset): Stream[IO, FailedElemLog] =
        sql"""SELECT * from public.failed_elem_logs WHERE
           |${andClause(selector)}
           |AND ordering > $offset
           |ORDER BY ordering ASC""".stripMargin
          .query[FailedElemLog]
          .streamWithChunkSize(config.batchSize)
          .transact(xas.read)

      override def count(selector: ProjectionSelector, timeRange: TimeRange): IO[Long] =
        sql"SELECT count(ordering) from public.failed_elem_logs ${whereClause(selector, timeRange)}"
          .query[Long]
          .unique
          .transact(xas.read)

      override def list(
          selector: ProjectionSelector,
          pagination: FromPagination,
          timeRange: TimeRange
      ): IO[List[FailedElemLog]] =
        sql"""SELECT * from public.failed_elem_logs
             |${whereClause(selector, timeRange)}
             |ORDER BY ordering ASC
             |LIMIT ${pagination.size} OFFSET ${pagination.from}""".stripMargin
          .query[FailedElemLog]
          .to[List]
          .transact(xas.read)

      def latest(size: Int): IO[List[FailedElemLog]] =
        sql""" SELECT * from public.failed_elem_logs
             | ORDER BY ordering DESC
             | LIMIT $size
             |""".stripMargin
          .query[FailedElemLog]
          .to[List]
          .transact(xas.read)

      private def andClause(selector: ProjectionSelector): Fragment =
        selector match {
          case ProjectionSelector.Name(value)            => fr"projection_name = $value"
          case ProjectionSelector.ProjectId(project, id) =>
            Fragments.and(
              fr"projection_project = $project",
              fr"projection_id = $id"
            )
        }

      private def whereClause(selector: ProjectionSelector, timeRange: TimeRange): Fragment = Fragments.whereAndOpt(
        Some(andClause(selector)),
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
