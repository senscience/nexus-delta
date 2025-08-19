package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.{FailedElemLog, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import cats.effect.{Clock, IO}
import fs2.Stream

trait ProjectionErrors {

  /**
    * Saves a list of failed elems
    *
    * @param metadata
    *   the metadata of the projection
    * @param failures
    *   the FailedElem to save
    */
  def saveFailedElems(metadata: ProjectionMetadata, failures: List[FailedElem]): IO[Unit]

  /**
    * Get available failed elem entries for a given projection by projection name, starting from a failed elem offset.
    *
    * @param projectionName
    *   the name of the projection
    * @param offset
    *   failed elem offset
    * @return
    */
  def failedElemEntries(projectionName: String, offset: Offset): Stream[IO, FailedElemLog]

  /**
    * Returns the total number of elems in the given time range
    */
  def count(timeRange: TimeRange): IO[Long]

  /**
    * Return the total of errors for the given projection on a time window ordered by instant
    *
    * @param project
    *   the project of the projection
    * @param projectionId
    *   its identifier
    * @param timeRange
    *   the time range to restrict on
    */
  def count(project: ProjectRef, projectionId: Iri, timeRange: TimeRange): IO[Long]

  /**
    * Return a list of errors for the given projection on a time window ordered by instant
    *
    * @param project
    *   the project of the projection
    * @param projectionId
    *   its identifier
    * @param pagination
    *   the pagination to apply
    * @param timeRange
    *   the time range to restrict on
    */
  def list(
      project: ProjectRef,
      projectionId: Iri,
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
    *   the projection
    */
  def deleteEntriesForProjection(projectionName: String): IO[Unit]

}

object ProjectionErrors {

  def apply(xas: Transactors, config: QueryConfig, clock: Clock[IO]): ProjectionErrors =
    new ProjectionErrors {

      private val store = FailedElemLogStore(xas, config, clock)

      override def saveFailedElems(metadata: ProjectionMetadata, failures: List[FailedElem]): IO[Unit] =
        store.save(metadata, failures)

      override def failedElemEntries(projectionName: String, offset: Offset): Stream[IO, FailedElemLog] =
        store.stream(projectionName, offset)

      override def count(timeRange: TimeRange): IO[Long] =
        store.count(timeRange)

      override def count(project: ProjectRef, projectionId: Iri, timeRange: TimeRange): IO[Long] =
        store.count(project, projectionId, timeRange)

      override def list(
          project: ProjectRef,
          projectionId: Iri,
          pagination: FromPagination,
          timeRange: TimeRange
      ): IO[List[FailedElemLog]] = store.list(project, projectionId, pagination, timeRange)

      override def latest(size: Int): IO[List[FailedElemLog]] =
        store.latest(size)

      override def deleteEntriesForProjection(projectionName: String): IO[Unit] =
        store.deleteEntriesForProjection(projectionName)
    }

}
