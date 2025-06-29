package ai.senscience.nexus.delta.sourcing.tombstone

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.tombstone.EventTombstoneStore.Value
import ai.senscience.nexus.testkit.mu.NexusSuite
import doobie.syntax.all.*
import munit.AnyFixture

class EventTombstoneStoreSuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobieTruncateAfterTest)

  private lazy val xas            = doobieTruncateAfterTest()
  private lazy val tombstoneStore = new EventTombstoneStore(xas)

  test("Save an event tombstone for the given identifier") {
    val tpe     = EntityType("test")
    val project = ProjectRef.unsafe("org", "project")
    val id      = nxv + "id"
    val subject = User("user", Label.unsafe("realm"))

    tombstoneStore.save(tpe, project, id, subject).transact(xas.write) >>
      tombstoneStore.count.assertEquals(1L) >>
      tombstoneStore
        .unsafeGet(project, id)
        .map(_.map(_.value))
        .assertEquals(Some(Value(subject)))
  }
}
