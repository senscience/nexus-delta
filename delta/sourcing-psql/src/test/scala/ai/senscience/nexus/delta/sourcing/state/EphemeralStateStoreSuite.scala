package ai.senscience.nexus.delta.sourcing.state

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.Message.MessageState
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.{DeleteExpired, Message}
import ai.senscience.nexus.testkit.mu.NexusSuite
import doobie.syntax.all.*
import munit.AnyFixture

import java.time.Instant
import scala.concurrent.duration.*

class EphemeralStateStoreSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions {
  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private lazy val store = EphemeralStateStore[Iri, MessageState](
    Message.entityType,
    MessageState.serializer,
    5.seconds,
    xas
  )

  private val project1 = ProjectRef.unsafe("org", "proj1")

  private val alice = User("Alice", Label.unsafe("Wonderland"))

  private val m1       = nxv + "m1"
  private val message1 = MessageState(m1, project1, "Hello, world !", alice, Instant.EPOCH, Anonymous)

  private val m2       = nxv + "m2"
  private val message2 = MessageState(m2, project1, "Bye !", alice, Instant.EPOCH.plusSeconds(60L), Anonymous)

  private lazy val deleteExpired = new DeleteExpired(xas)

  test("save the states") {
    for {
      _ <- store.save(message1).transact(xas.write).assert
      _ <- store.save(message2).transact(xas.write).assert
    } yield ()
  }

  test("get the states") {
    for {
      _ <- store.get(project1, m1).assertEquals(Some(message1))
      _ <- store.get(project1, m2).assertEquals(Some(message2))
      _ <- store.get(project1, nxv + "mx").assertEquals(None)
    } yield ()
  }

  test("delete expired state " + m1) {
    val threshold = Instant.EPOCH.plusSeconds(6L)
    for {
      _ <- deleteExpired(threshold)
      _ <- store.get(project1, m1).assertEquals(None)
      _ <- store.get(project1, m2).assertEquals(Some(message2))
    } yield ()
  }
}
