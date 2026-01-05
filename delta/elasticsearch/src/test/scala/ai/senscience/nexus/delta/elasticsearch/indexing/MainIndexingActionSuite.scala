package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.Fixtures
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.generators.ResourceGen
import ai.senscience.nexus.delta.sdk.indexing.sync.SyncIndexingOutcome
import ai.senscience.nexus.delta.sdk.indexing.{MainDocument, MainDocumentEncoder}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.stream.NoopSink
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import io.circe.Json

import scala.concurrent.duration.*

class MainIndexingActionSuite extends NexusSuite with Fixtures {

  private val project = ProjectRef.unsafe("org", "proj")

  private val entityType = EntityType("test")

  private val id  = nxv + "id1"
  private val res = ResourceGen.resourceFUnit(id, project, Set(nxv + "Test"))

  private val exception = new IllegalStateException("Boom")

  private val mainAggregateEncoder = new MainDocumentEncoder.Aggregate {
    override def fromJson(entityType: EntityType)(json: Json): IO[MainDocument] = IO.stub

    override def fromResource[A](tpe: EntityType)(resource: ResourceF[A]): IO[MainDocument] =
      tpe match {
        case `entityType` => IO.pure(MainDocument.unsafe(Json.obj()))
        case _            => IO.raiseError(exception)
      }
  }

  private val mainIndexingAction = new MainIndexingAction(mainAggregateEncoder, new NoopSink[Json], 5.seconds)

  test("A valid elem should be indexed") {
    mainIndexingAction(entityType)(project, res).assertEquals(SyncIndexingOutcome.Success)
  }

  test("A failed elem should be returned") {
    mainIndexingAction(EntityType("xxx"))(project, res)
      .assertEquals(SyncIndexingOutcome.Failed(List(exception)))
  }
}
