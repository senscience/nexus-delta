package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.config.{PurgeConfig, QueryConfig}
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.model.{IndexingStatus, ProjectionRestart}
import ai.senscience.nexus.delta.sourcing.query.{SelectFilter, StreamingQuery}
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import ai.senscience.nexus.delta.sourcing.stream.{ProjectionMetadata, ProjectionProgress, ProjectionStore}
import ai.senscience.nexus.delta.sourcing.{EntityCheck, ProgressStatistics, Scope, Transactors}
import cats.data.NonEmptyList
import cats.effect.{Clock, IO}
import cats.syntax.order.catsSyntaxPartialOrder
import fs2.Stream

import java.time.Instant

trait Projections {

  /**
    * Retrieves a projection progress if found.
    *
    * @param name
    *   the name of the projection
    */
  def progress(name: String): IO[Option[ProjectionProgress]]

  /**
    * Retrieves the offset for a given projection.
    *
    * @param name
    *   the name of the projection
    */
  def offset(name: String): IO[Offset] = progress(name).map(_.fold[Offset](Offset.start)(_.offset))

  /**
    * Saves a projection offset.
    *
    * @param metadata
    *   the metadata of the projection
    * @param progress
    *   the offset to save
    */
  def save(metadata: ProjectionMetadata, progress: ProjectionProgress): IO[Unit]

  /**
    * Returns the indexing status for the given resource in the given projection
    */
  def indexingStatus[E <: Throwable](
      project: ProjectRef,
      selectFilter: SelectFilter,
      projection: String,
      resourceId: Iri
  )(notFound: => Throwable): IO[IndexingStatus]

  /**
    * Resets the progress of a projection to the given offset, and the instants (createdAt, updatedAt) to the time of
    * the reset
    */
  def reset(name: String, offset: Offset): IO[Unit]

  /**
    * Deletes a projection offset if found.
    *
    * @param name
    *   the name of the projection
    */
  def delete(name: String): IO[Unit]

  /**
    * Schedules a restart for the given projection at the given offset
    */
  def scheduleRestart(projectionName: String, fromOffset: Offset)(using subject: Subject): IO[Unit]

  /**
    * Get scheduled projection restarts from a given offset
    * @param offset
    *   the offset to start from
    */
  def restarts(offset: Offset): Stream[IO, (Offset, ProjectionRestart)]

  /**
    * Acknowledge that a restart has been performed
    * @param id
    *   the identifier of the restart
    * @return
    */
  def acknowledgeRestart(id: Offset): IO[Unit]

  /**
    * Deletes projection restarts older than the configured period
    */
  def deleteExpiredRestarts(instant: Instant): IO[Unit]

  /**
    * Returns the statistics for the given projection in the given project
    *
    * @param scope
    *   the scope of statistics
    * @param selectFilter
    *   what to filter for
    * @param projectionId
    *   the projection id for which the statistics are computed
    */
  def statistics(scope: Scope, selectFilter: SelectFilter, projectionId: String): IO[ProgressStatistics]
}

object Projections {

  private val logger = Logger[Projections]

  def apply(
      xas: Transactors,
      entityTypes: Option[NonEmptyList[EntityType]],
      config: QueryConfig,
      clock: Clock[IO]
  ): Projections =
    new Projections {
      private val projectionStore        = ProjectionStore(xas, config, clock)
      private val projectionRestartStore = new ProjectionRestartStore(xas, config)

      override def progress(name: String): IO[Option[ProjectionProgress]] = projectionStore.offset(name)

      override def save(metadata: ProjectionMetadata, progress: ProjectionProgress): IO[Unit] =
        projectionStore.save(metadata, progress)

      def indexingStatus[E <: Throwable](
          project: ProjectRef,
          selectFilter: SelectFilter,
          projection: String,
          resourceId: Iri
      )(notFound: => Throwable): IO[IndexingStatus] =
        for {
          projectionOffset <- offset(projection)
          resourceOffset   <- StreamingQuery.offset(Scope(project), entityTypes, selectFilter, resourceId, xas)
          status           <- resourceOffset match {
                                case Some(o) => IO.pure(IndexingStatus.fromOffsets(projectionOffset, o))
                                case None    =>
                                  EntityCheck.findType(resourceId, project, xas).flatMap {
                                    case Some(_) => IO.pure(IndexingStatus.Discarded)
                                    case None    => IO.raiseError(notFound)
                                  }
                              }
        } yield status

      override def reset(name: String, offset: Offset): IO[Unit] = projectionStore.reset(name, offset)

      override def delete(name: String): IO[Unit] = projectionStore.delete(name)

      override def scheduleRestart(projectionName: String, fromOffset: Offset)(using subject: Subject): IO[Unit] =
        offset(projectionName).flatMap {
          case currentOffset if currentOffset >= fromOffset =>
            logger.debug(s"'$projectionName' has a greater offset, scheduling a restart...") >>
              clock.realTimeInstant.flatMap { now =>
                projectionRestartStore.save(ProjectionRestart(projectionName, fromOffset, now, subject))
              }
          case _                                            =>
            logger.debug(s"'$projectionName' has a smaller offset, skipping...")
        }

      override def restarts(offset: Offset): Stream[IO, (Offset, ProjectionRestart)] =
        projectionRestartStore.stream(offset)

      override def acknowledgeRestart(id: Offset): IO[Unit] = projectionRestartStore.acknowledge(id)

      override def deleteExpiredRestarts(instant: Instant): IO[Unit] = projectionRestartStore.deleteExpired(instant)

      override def statistics(
          scope: Scope,
          selectFilter: SelectFilter,
          projectionId: String
      ): IO[ProgressStatistics] =
        for {
          current   <- progress(projectionId)
          remaining <-
            StreamingQuery.remaining(
              scope,
              entityTypes,
              selectFilter,
              current.fold(Offset.start)(_.offset),
              xas
            )
        } yield ProgressStatistics(current, remaining)
    }

  private val purgeRestartMetadata = ProjectionMetadata("system", "purge-projection-restarts", None, None)

  def purgeExpiredRestarts(projections: Projections, config: PurgeConfig): PurgeProjection =
    PurgeProjection(purgeRestartMetadata, config, projections.deleteExpiredRestarts)
}
