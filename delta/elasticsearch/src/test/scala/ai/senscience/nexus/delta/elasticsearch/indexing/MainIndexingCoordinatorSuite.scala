package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.Fixtures
import ai.senscience.nexus.delta.elasticsearch.indexing.MainIndexingCoordinator.ProjectDef
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.indexing.MainDocument
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sdk.stream.MainDocumentStream
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.SupervisorSetup.unapply
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.IO
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.circe.Json
import munit.AnyFixture

import java.time.Instant
import scala.concurrent.duration.*

class MainIndexingCoordinatorSuite extends NexusSuite with SupervisorSetup.Fixture with Fixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(supervisor)

  private given PatienceConfig = PatienceConfig(10.seconds, 10.millis)

  private lazy val (sv, projections, _) = unapply(supervisor())

  private val project1   = ProjectRef.unsafe("org", "proj1")
  private val project1Id = Projects.encodeId(project1)

  private val project2   = ProjectRef.unsafe("org", "proj2")
  private val project2Id = Projects.encodeId(project1)

  private val resumeSignal = SignallingRef[IO, Boolean](false).unsafeRunSync()

  private val entityType = EntityType("test")

  private def success[A](entityType: EntityType, project: ProjectRef, id: Iri, value: A, offset: Long): Elem[A] =
    SuccessElem(tpe = entityType, id, project, Instant.EPOCH, Offset.at(offset), value, 1)

  // Stream 2 elements until signal is set to true and then 2 more
  private def projectStream: ElemStream[ProjectDef] =
    Stream(
      success(Projects.entityType, project1, project1Id, ProjectDef(project1, markedForDeletion = false), 1L),
      success(Projects.entityType, project2, project2Id, ProjectDef(project2, markedForDeletion = false), 2L)
    ) ++ Stream.never[IO].interruptWhen(resumeSignal) ++
      Stream(
        success(Projects.entityType, project1, project1Id, ProjectDef(project1, markedForDeletion = false), 3L),
        success(Projects.entityType, project2, project2Id, ProjectDef(project2, markedForDeletion = true), 4L)
      )

  private val mainDocumentStream = new MainDocumentStream {
    override def continuous(project: ProjectRef, start: Offset): ElemStream[MainDocument] =
      Stream.range(1, 3).map { i =>
        success(entityType, project, nxv + i.toString, MainDocument.unsafe(json"""{ "value": "$i"}"""), i)
      } ++ Stream(
        DroppedElem(entityType, nxv + "3", project, Instant.EPOCH, Offset.at(3L), 1),
        FailedElem(
          entityType,
          nxv + "4",
          project,
          Instant.EPOCH,
          Offset.at(4L),
          new IllegalStateException("This is an error message"),
          1
        )
      )
  }

  private val expectedProgress = ProjectionProgress(Offset.at(4L), Instant.EPOCH, 4, 1, 1)

  test("Start the coordinator") {
    for {
      _ <- MainIndexingCoordinator(
             _ => projectStream,
             mainDocumentStream,
             sv,
             IO.unit,
             new NoopSink[Json]
           )
      _ <- sv.describe(MainIndexingCoordinator.metadata.name)
             .map(_.map(_.progress))
             .assertEquals(Some(ProjectionProgress(Offset.at(2L), Instant.EPOCH, 2, 0, 0)))
             .eventually
    } yield ()
  }

  test(s"Projection for '$project1' processed all items and completed") {
    val projectionName = s"main-indexing-$project1"
    for {
      _ <- sv.describe(projectionName)
             .map(_.map(_.status))
             .assertEquals(Some(ExecutionStatus.Completed))
             .eventually
      _ <- projections.progress(projectionName).assertEquals(Some(expectedProgress))
    } yield ()
  }

  test(s"Projection for '$project2' processed all items and completed too") {
    val projectionName = s"main-indexing-$project2"
    for {
      _ <- sv.describe(projectionName)
             .map(_.map(_.status))
             .assertEquals(Some(ExecutionStatus.Completed))
             .eventually
      _ <- projections.progress(projectionName).assertEquals(Some(expectedProgress))
    } yield ()
  }

  test("Resume the stream of projects") {
    for {
      _ <- resumeSignal.set(true)
      _ <- sv.describe(MainIndexingCoordinator.metadata.name)
             .map(_.map(_.progress))
             .assertEquals(Some(ProjectionProgress(Offset.at(4L), Instant.EPOCH, 4, 0, 0)))
             .eventually
    } yield ()
  }

  test(s"Projection for '$project1' should not be restarted by the new project state.") {
    val projectionName = s"main-indexing-$project1"
    for {
      _ <- sv.describe(projectionName).map(_.map(_.restarts)).assertEquals(Some(0)).eventually
      _ <- projections.progress(projectionName).assertEquals(Some(expectedProgress))
    } yield ()
  }

  test(s"'$project2' is marked for deletion, the associated projection should be destroyed.") {
    val projectionName = s"main-indexing-$project2"
    for {
      _ <- sv.describe(projectionName).assertEquals(None).eventually
      _ <- projections.progress(projectionName).assertEquals(None)
    } yield ()
  }
}
