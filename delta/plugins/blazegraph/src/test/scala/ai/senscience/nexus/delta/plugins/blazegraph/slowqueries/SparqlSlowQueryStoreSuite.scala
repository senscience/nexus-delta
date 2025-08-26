package ai.senscience.nexus.delta.plugins.blazegraph.slowqueries

import ai.senscience.nexus.delta.kernel.search.{Pagination, TimeRange}
import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.model.SparqlSlowQuery
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.concurrent.duration.DurationInt

class SparqlSlowQueryStoreSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions {
  override def munitFixtures: Seq[AnyFixture[?]] = List(doobieTruncateAfterTest)

  private lazy val xas   = doobieTruncateAfterTest()
  private lazy val store = SparqlSlowQueryStore(xas)

  private val view = ViewRef.unsafe("senscience", "atoll", nxv + "id")

  private def listAll = store.list(Pagination.OnePage, TimeRange.Anytime)

  test("Save a slow query") {

    val slowQuery = SparqlSlowQuery(
      view,
      SparqlQuery(""),
      failed = true,
      1.second,
      Instant.EPOCH,
      Identity.User("Ted Lasso", Label.unsafe("epfl"))
    )

    store.save(slowQuery) >>
      listAll.assertEquals(List(slowQuery))
  }

  test("Remove old queries") {
    def queryAtTime(instant: Instant): SparqlSlowQuery =
      SparqlSlowQuery(
        view,
        SparqlQuery(""),
        failed = false,
        1.second,
        instant,
        Identity.User("Alice", Label.unsafe("senscience"))
      )

    val now          = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    val oneWeekAgo   = now.minus(Duration.ofDays(7))
    val eightDaysAgo = now.minus(Duration.ofDays(8))
    val recentQuery  = queryAtTime(now)
    val oldQuery     = queryAtTime(eightDaysAgo)

    for {
      _       <- store.save(oldQuery)
      _       <- store.save(recentQuery)
      _       <- store.deleteExpired(oneWeekAgo)
      results <- listAll
    } yield {
      assert(results.contains(recentQuery), "recent query was deleted")
      assert(!results.contains(oldQuery), "old query was not deleted")
    }
  }
}
