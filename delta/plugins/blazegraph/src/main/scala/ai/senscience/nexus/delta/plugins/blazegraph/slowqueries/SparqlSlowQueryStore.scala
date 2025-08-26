package ai.senscience.nexus.delta.plugins.blazegraph.slowqueries

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.model.SparqlSlowQuery
import ai.senscience.nexus.delta.sourcing.{FragmentEncoder, Transactors}
import ai.senscience.nexus.delta.sourcing.config.PurgeConfig
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import cats.effect.IO
import doobie.Fragments
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import io.circe.syntax.EncoderOps

import java.time.Instant

/**
  * Persistence operations for slow SPARQL query logs
  */
trait SparqlSlowQueryStore {

  /**
    * Returns the total number of slow queries in the given time range
    */
  def count(timeRange: TimeRange): IO[Long]

  /**
    * Saves a slow query
    */
  def save(query: SparqlSlowQuery): IO[Unit]

  /**
    * List slow queries on a time window
    */
  def list(pagination: FromPagination, timeRange: TimeRange): IO[List[SparqlSlowQuery]]

  /**
    * Delete slow queries older than the given instant
    */
  def deleteExpired(instant: Instant): IO[Unit]
}

object SparqlSlowQueryStore {
  def apply(xas: Transactors): SparqlSlowQueryStore = {
    new SparqlSlowQueryStore {

      implicit val timeRangeFragmentEncoder: FragmentEncoder[TimeRange] = createTimeRangeFragmentEncoder("instant")

      override def count(timeRange: TimeRange): IO[Long] =
        sql"SELECT count(ordering) FROM public.blazegraph_queries ${whereInstant(timeRange)}"
          .query[Long]
          .unique
          .transact(xas.read)

      override def save(query: SparqlSlowQuery): IO[Unit] =
        sql"""| INSERT INTO blazegraph_queries(project, view_id, instant, duration, subject, query, failed)
              | VALUES(${query.view.project}, ${query.view.viewId}, ${query.instant}, ${query.duration}, ${query.subject.asJson}, ${query.query.value}, ${query.failed})
        """.stripMargin.update.run.transact(xas.write).void

      override def list(pagination: FromPagination, timeRange: TimeRange): IO[List[SparqlSlowQuery]] =
        sql"""| SELECT project, view_id, instant, duration, subject, query, failed
              | FROM public.blazegraph_queries
              | ${whereInstant(timeRange)}
              | ORDER BY instant ASC
              | LIMIT ${pagination.size} OFFSET ${pagination.from}""".stripMargin
          .query[SparqlSlowQuery]
          .to[List]
          .transact(xas.read)

      override def deleteExpired(instant: Instant): IO[Unit] =
        sql"""| DELETE FROM public.blazegraph_queries
              | WHERE instant < $instant
           """.stripMargin.update.run.transact(xas.write).void

      private def whereInstant(timeRange: TimeRange) =
        Fragments.whereAndOpt(timeRange.asFragment)
    }
  }

  private val metadata: ProjectionMetadata =
    ProjectionMetadata("system", "sparql-slow-query-log-deletion", None, None)

  def deleteExpired(config: PurgeConfig, store: SparqlSlowQueryStore) =
    PurgeProjection(metadata, config, store.deleteExpired)
}
