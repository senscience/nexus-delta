package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.indexing.ProjectDefCoordinator.ProjectDef
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.SupervisorSetup.unapply
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.IO
import fs2.Stream
import fs2.concurrent.SignallingRef
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.*

class ProjectDefCoordinatorSuite extends NexusSuite with SupervisorSetup.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(supervisor)

  private given PatienceConfig = PatienceConfig(10.seconds, 10.millis)

  private lazy val (sv, projections, _) = unapply(supervisor())

  private val project1   = ProjectRef.unsafe("org", "proj1")
  private val project1Id = Projects.encodeId(project1)

  private val project2   = ProjectRef.unsafe("org", "proj2")
  private val project2Id = Projects.encodeId(project1)

  private val resumeSignal = SignallingRef[IO, Boolean](false).unsafeRunSync()

  private val entityType = EntityType("test")

  private def success[A](entityType: EntityType, project: ProjectRef, id: Iri, value: A, offset: Long): SuccessElem[A] =
    SuccessElem(tpe = entityType, id, project, Instant.EPOCH, Offset.at(offset), value, 1)

  private def projectStream: SuccessElemStream[ProjectDef] =
    Stream(
      success(Projects.entityType, project1, project1Id, ProjectDef(project1, markedForDeletion = false), 1L),
      success(Projects.entityType, project2, project2Id, ProjectDef(project2, markedForDeletion = false), 2L)
    ) ++ Stream.never[IO].interruptWhen(resumeSignal) ++
      Stream(
        success(Projects.entityType, project1, project1Id, ProjectDef(project1, markedForDeletion = false), 3L),
        success(Projects.entityType, project2, project2Id, ProjectDef(project2, markedForDeletion = true), 4L)
      )

  private def projectionName(project: ProjectRef): String = s"project-$project"

  private def projectionStream(project: ProjectRef) =
    (_: Offset) =>
      Stream.range(1, 3).map { i =>
        success(entityType, project, nxv + i.toString, (), i)
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

  private val projectProjectionFactory = new ProjectProjectionFactory {
    override def bootstrap: IO[Unit] = IO.unit

    override def name(project: ProjectRef): String = projectionName(project)

    override def onInit(project: ProjectRef): IO[Unit] = IO.unit

    override def compile(project: ProjectRef): IO[CompiledProjection] = IO.pure(
      CompiledProjection.fromStream(
        ProjectionMetadata(
          "main-indexing",
          projectionName(project),
          Some(project),
          Some(nxv + "projection")
        ),
        ExecutionStrategy.PersistentSingleNode,
        projectionStream(project)
      )
    )
  }

  private val expectedProgress = ProjectionProgress(Offset.at(4L), Instant.EPOCH, 4, 1, 1)

  private def assertCompleted(project: ProjectRef)(using Location) = {
    val name = projectionName(project)
    sv.describe(name)
      .map(_.map(_.status))
      .assertEquals(Some(ExecutionStatus.Completed))
      .eventually
  }

  private def assertCoordinatorProgress(expected: ProjectionProgress)(using Location) =
    sv.describe(ProjectDefCoordinator.metadata.name)
      .map(_.map(_.progress))
      .assertEquals(Some(expected))
      .eventually

  test("Start the coordinator") {
    ProjectDefCoordinator(sv, _ => projectStream, Set(projectProjectionFactory)) >>
      assertCoordinatorProgress(ProjectionProgress(Offset.at(2L), Instant.EPOCH, 2, 0, 0))
  }

  test(s"Projection for '$project1' processed all items and completed") {
    assertCompleted(project1) >>
      projections.progress(projectionName(project1)).assertEquals(Some(expectedProgress))
  }

  test(s"Projection for '$project2' processed all items and completed too") {
    assertCompleted(project2) >>
      projections.progress(projectionName(project2)).assertEquals(Some(expectedProgress))
  }

  test("Resume the stream of projects") {
    resumeSignal.set(true) >>
      assertCoordinatorProgress(ProjectionProgress(Offset.at(4L), Instant.EPOCH, 4, 0, 0))
  }

  test(s"Projection for '$project1' should not be restarted by the new project state.") {
    val name = projectionName(project1)
    for {
      _ <- sv.describe(name).map(_.map(_.restarts)).assertEquals(Some(0)).eventually
      _ <- projections.progress(name).assertEquals(Some(expectedProgress))
    } yield ()
  }

  test(s"'$project2' is marked for deletion, the associated projection should be destroyed.") {
    val name = projectionName(project2)
    for {
      _ <- sv.describe(name).assertEquals(None).eventually
      _ <- projections.progress(name).assertEquals(None)
    } yield ()
  }

}
