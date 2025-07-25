package ai.senscience.nexus.delta.plugins.compositeviews.store

import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeRestart
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeRestart.*
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

import java.time.Instant

class CompositeRestartStoreSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private lazy val store = new CompositeRestartStore(xas)

  private val proj = ProjectRef.unsafe("org", "proj")

  private val id1     = nxv + "id1"
  private val viewRef = ViewRef(proj, id1)

  private val cr1 = FullRestart(viewRef, Instant.EPOCH, Anonymous)

  private val id2      = nxv + "id2"
  private val viewRef2 = ViewRef(proj, id2)
  private val cr2      = FullRebuild(viewRef2, Instant.EPOCH, Anonymous)

  private val projection = nxv + "projection"
  private val cr3        = PartialRebuild(viewRef, projection, Instant.EPOCH.plusSeconds(5L), Anonymous)

  private def toElem(offset: Offset, restart: CompositeRestart) =
    SuccessElem(entityType, restart.id, restart.project, restart.instant, offset, restart, 1)

  test("Save composite restarts") {
    for {
      _ <- store.save(cr1).assert
      _ <- store.save(cr2).assert
      _ <- store.save(cr3).assert
    } yield ()
  }

  test("Get first restart") {
    store.head(viewRef).assertEquals(Some(toElem(Offset.at(1L), cr1)))
  }

  test("Delete older restarts and get first restart again") {
    for {
      _ <- store.deleteExpired(Instant.EPOCH.plusSeconds(2L))
      _ <- store.head(viewRef).assertEquals(Some(toElem(Offset.at(3L), cr3)))
    } yield ()
  }

  test("Acknowledge restart 3 and get first restart again") {
    for {
      _ <- store.acknowledge(Offset.at(3L))
      _ <- store.head(viewRef).assertEquals(None)
    } yield ()
  }

}
