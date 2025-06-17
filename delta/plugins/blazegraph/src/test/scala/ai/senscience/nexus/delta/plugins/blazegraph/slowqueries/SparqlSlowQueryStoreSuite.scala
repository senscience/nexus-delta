package ai.senscience.nexus.delta.plugins.blazegraph.slowqueries

import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.SparqlSlowQueryStoreSuite.{view, OldQuery, OneWeekAgo, RecentQuery}
import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.model.SparqlSlowQuery
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ch.epfl.bluebrain.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.concurrent.duration.DurationInt

class SparqlSlowQueryStoreSuite
    extends NexusSuite
    with Doobie.Fixture
    with Doobie.Assertions
    with BlazegraphSlowQueryStoreFixture {
  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie, blazegraphSlowQueryStore)

  private lazy val store = blazegraphSlowQueryStore()

  test("Save a slow query") {

    val slowQuery = SparqlSlowQuery(
      view,
      SparqlQuery(""),
      failed = true,
      1.second,
      Instant.now().truncatedTo(ChronoUnit.MILLIS),
      Identity.User("Ted Lasso", Label.unsafe("epfl"))
    )

    for {
      _      <- store.save(slowQuery)
      lookup <- store.listForTestingOnly(view)
    } yield {
      assertEquals(lookup, List(slowQuery))
    }
  }

  test("Remove old queries") {
    for {
      _       <- store.save(OldQuery)
      _       <- store.save(RecentQuery)
      _       <- store.removeQueriesOlderThan(OneWeekAgo)
      results <- store.listForTestingOnly(view)
    } yield {
      assert(results.contains(RecentQuery), "recent query was deleted")
      assert(!results.contains(OldQuery), "old query was not deleted")
    }
  }
}

object SparqlSlowQueryStoreSuite {
  private val view                                           = ViewRef(ProjectRef.unsafe("epfl", "blue-brain"), Iri.unsafe("brain"))
  private def queryAtTime(instant: Instant): SparqlSlowQuery = {
    SparqlSlowQuery(
      view,
      SparqlQuery(""),
      failed = false,
      1.second,
      instant,
      Identity.User("Ted Lasso", Label.unsafe("epfl"))
    )
  }

  private val Now          = Instant.now().truncatedTo(ChronoUnit.MILLIS)
  private val OneWeekAgo   = Now.minus(Duration.ofDays(7))
  private val EightDaysAgo = Now.minus(Duration.ofDays(8))
  private val RecentQuery  = queryAtTime(Now)
  private val OldQuery     = queryAtTime(EightDaysAgo)
}
