package ai.senscience.nexus.delta.sourcing.event

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestEvent.{PullRequestCreated, PullRequestMerged, PullRequestUpdated}
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import doobie.syntax.all.*
import munit.AnyFixture

import java.time.Instant

class ScopedEventStoreSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions {

  private val queryConfig = QueryConfig.stopping(10)

  private lazy val fixture = doobieInject(PullRequest.eventStore(_, queryConfig, allEvents*))

  override def munitFixtures: Seq[AnyFixture[?]] = List(fixture)

  private val project1 = ProjectRef.unsafe("org", "proj1")
  private val project2 = ProjectRef.unsafe("org", "proj2")
  private val project3 = ProjectRef.unsafe("org2", "proj3")

  private val id1 = nxv + "1"
  private val id2 = nxv + "2"
  private val id3 = nxv + "3"

  private val event1 = PullRequestCreated(id1, project1, Instant.EPOCH, Anonymous)
  private val event2 = PullRequestUpdated(id1, project1, 2, Instant.EPOCH, User("Alice", Label.unsafe("Wonderland")))
  private val event3 = PullRequestMerged(id1, project1, 3, Instant.EPOCH, User("Alice", Label.unsafe("Wonderland")))

  private val event4 = PullRequestCreated(id2, project1, Instant.EPOCH, Anonymous)

  private val event5 = PullRequestCreated(id1, project2, Instant.EPOCH, Anonymous)

  private val event6 = PullRequestCreated(id3, project3, Instant.EPOCH, Anonymous)

  private val allEvents = List(event1, event2, event3, event4, event5, event6)

  private lazy val (xas, store) = fixture()

  private def assertCount = sql"select count(*) from scoped_events".query[Int].unique.transact(xas.read).assertEquals(6)

  test("Fail when the PK already exists") {
    for {
      _ <- store
             .save(PullRequestMerged(id1, project1, 2, Instant.EPOCH, Anonymous))
             .transact(xas.write)
             .expectUniqueViolation
      _ <- assertCount
    } yield ()
  }

  test("Fetch all events for a given id") {
    store.history(project1, id1).transact(xas.read).assert(event1, event2, event3)
  }

  test("Fetch all events for a given id up to revision 2") {
    store.history(project1, id1, 2).transact(xas.read).assert(event1, event2)
  }

  test("Get an empty stream for an unknown (project, id)") {
    store.history(project2, id2, 2).transact(xas.read).assertEmpty
  }

  test(s"Delete all events for id $id1 in project $project1") {
    store.deleteAll(project1, id1).transact(xas.write) >>
      store.history(project1, id2).transact(xas.read).assert(event4)
    store.history(project2, id1).transact(xas.read).assert(event5)
  }
}
