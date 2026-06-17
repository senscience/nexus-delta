package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.indexing.ElasticSearchRunningStore.ElasticRunningView
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

import java.util.UUID

class ElasticSearchRunningStoreSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas   = doobie()
  private lazy val store = ElasticSearchRunningStore(xas)

  private val project = ProjectRef.unsafe("org", "proj")
  private val ref1    = ViewRef(project, nxv + "view1")
  private val ref2    = ViewRef(project, nxv + "view2")

  private val rev1 = IndexingRev.init
  private val rev2 = IndexingRev(2)

  private val view1Rev1 = ElasticRunningView(ref1, rev1, UUID.randomUUID())
  private val view1Rev2 = ElasticRunningView(ref1, rev2, UUID.randomUUID())
  private val view2Rev1 = ElasticRunningView(ref2, rev1, UUID.randomUUID())

  test("Return an empty list for an unknown view") {
    store.list(ref1).assertEquals(List.empty)
  }

  test("Save a view revision and list it back") {
    store.save(view1Rev1) >>
      store.list(ref1).assertEquals(List(view1Rev1))
  }

  test("Save a second revision of the same view") {
    store.save(view1Rev2) >>
      store.list(ref1).map(_.toSet).assertEquals(Set(view1Rev1, view1Rev2))
  }

  test("Only list the revisions of the requested view") {
    store.save(view2Rev1) >>
      store.list(ref2).assertEquals(List(view2Rev1))
  }

  test("Delete a single revision, leaving the others untouched") {
    for {
      _ <- store.delete(ref1, rev1)
      _ <- store.list(ref1).assertEquals(List(view1Rev2))
      _ <- store.list(ref2).assertEquals(List(view2Rev1))
    } yield ()
  }

  test("Deleting an unknown revision is a no-op") {
    for {
      _ <- store.delete(ref1, IndexingRev(42))
      _ <- store.list(ref1).assertEquals(List(view1Rev2))
    } yield ()
  }

}
