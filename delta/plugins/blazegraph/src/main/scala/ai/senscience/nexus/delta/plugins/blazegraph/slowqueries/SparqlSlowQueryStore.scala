package ai.senscience.nexus.delta.plugins.blazegraph.slowqueries

import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.model.SparqlSlowQuery
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
import cats.effect.IO
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import io.circe.syntax.EncoderOps

import java.time.Instant

/**
  * Persistence operations for slow query logs
  */
trait SparqlSlowQueryStore {
  def save(query: SparqlSlowQuery): IO[Unit]
  def listForTestingOnly(view: ViewRef): IO[List[SparqlSlowQuery]]
  def removeQueriesOlderThan(instant: Instant): IO[Unit]
}

object SparqlSlowQueryStore {
  def apply(xas: Transactors): SparqlSlowQueryStore = {
    new SparqlSlowQueryStore {
      override def save(query: SparqlSlowQuery): IO[Unit] = {
        sql""" INSERT INTO blazegraph_queries(project, view_id, instant, duration, subject, query, failed)
             | VALUES(${query.view.project}, ${query.view.viewId}, ${query.instant}, ${query.duration}, ${query.subject.asJson}, ${query.query.value}, ${query.failed})
        """.stripMargin.update.run
          .transact(xas.write)
          .void
      }

      override def listForTestingOnly(view: ViewRef): IO[List[SparqlSlowQuery]] = {
        sql""" SELECT project, view_id, instant, duration, subject, query, failed FROM public.blazegraph_queries
             |WHERE view_id = ${view.viewId} AND project = ${view.project}
           """.stripMargin
          .query[SparqlSlowQuery]
          .stream
          .transact(xas.read)
          .compile
          .toList
      }

      override def removeQueriesOlderThan(instant: Instant): IO[Unit] = {
        sql""" DELETE FROM public.blazegraph_queries
             |WHERE instant < $instant
           """.stripMargin.update.run
          .transact(xas.write)
          .void
      }
    }
  }
}
