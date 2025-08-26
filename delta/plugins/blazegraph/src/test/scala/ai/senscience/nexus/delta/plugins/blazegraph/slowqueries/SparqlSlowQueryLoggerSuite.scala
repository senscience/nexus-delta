package ai.senscience.nexus.delta.plugins.blazegraph.slowqueries

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.{Pagination, TimeRange}
import ai.senscience.nexus.delta.kernel.search.TimeRange.Anytime
import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.model.SparqlSlowQuery
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.DurationInt

class SparqlSlowQueryLoggerSuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobieTruncateAfterTest)

  private val longQueryThreshold = 100.milliseconds

  private val view        = ViewRef.unsafe("senscience", "atoll", nxv + "id")
  private val sparqlQuery = SparqlQuery("SELECT ?s where {?s ?p ?o} LIMIT 20")
  private val user        = Identity.User("Alice", Label.unsafe("senscience"))

  private lazy val xas    = doobieTruncateAfterTest()
  private lazy val store  = SparqlSlowQueryStore(xas)
  private lazy val logger = SparqlSlowQueryLogger(store, longQueryThreshold, clock)

  private def findQuery = store.list(Pagination.OnePage, Anytime).map(_.headOption)

  private def assertSavedQuery(obtainedOpt: Option[SparqlSlowQuery], failed: Boolean)(implicit
      location: Location
  ): Unit =
    obtainedOpt match {
      case Some(obtained) =>
        assertEquals(obtained.view, view)
        assertEquals(obtained.query, sparqlQuery)
        assertEquals(obtained.subject, user)
        assertEquals(obtained.failed, failed)
        assertEquals(obtained.instant, Instant.EPOCH)
        assert(obtained.duration >= longQueryThreshold)
      case None           => fail(s"A query should have save as '$longQueryThreshold' was exceeded.")
    }

  test("Slow query is logged") {
    val slowQuery = IO.sleep(101.milliseconds)
    logger.save(view, sparqlQuery, user, slowQuery) >>
      findQuery.map(assertSavedQuery(_, failed = false))
  }

  test("Slow failure logged") {
    val slowFailingQuery = IO.sleep(101.milliseconds) >> IO.raiseError(new RuntimeException())

    logger.save(view, sparqlQuery, user, slowFailingQuery).attempt.assert(_.isLeft) >>
      findQuery.map(assertSavedQuery(_, failed = true))
  }

  test("Fast query is not logged") {
    val fastQuery = IO.sleep(50.milliseconds)

    logger.save(view, sparqlQuery, user, fastQuery) >>
      findQuery.assert(_.isEmpty, "This query should not have been logged")
  }

  test("Continue when saving slow query log fails") {
    val failingStore: SparqlSlowQueryStore = new SparqlSlowQueryStore {

      override def count(timeRange: TimeRange): IO[Long] = IO.pure(42L)

      override def save(query: SparqlSlowQuery): IO[Unit] =
        IO.raiseError(new IllegalStateException("error saving slow log"))

      override def deleteExpired(instant: Instant): IO[Unit] = IO.unit

      override def list(pagination: FromPagination, timeRange: TimeRange): IO[List[SparqlSlowQuery]] = IO.pure(Nil)
    }

    val logSlowQueries = SparqlSlowQueryLogger(failingStore, longQueryThreshold, clock)
    val query          = IO.sleep(101.milliseconds).as("result")

    logSlowQueries.save(view, sparqlQuery, user, query).assertEquals("result")
  }
}
