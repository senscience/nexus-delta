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
import ai.senscience.nexus.testkit.InvocationCounter
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

  private val project1 = ProjectRef.unsafe("org", "proj1")
  private val project2 = ProjectRef.unsafe("org", "proj2")
  // An inactive project: the coordinator should not start its projections from the state stream.
  private val project3 = ProjectRef.unsafe("org", "proj3")

  private val resumeSignal    = SignallingRef[IO, Boolean](false).unsafeRunSync()
  // project1 and project2 are active; project3 is not.
  private val projectActivity = ControllableProjectActivity(project1, project2).unsafeRunSync()
  // The activations topic the resumer reacts to; the tests publish project/projection activations to it.
  private val activations     = ProjectionActivations().unsafeRunSync()
  private val resumer         = ProjectDefResumer(projectActivity, activations)
  // Counts onInit invocations per project; lets the resume test assert restart deterministically (there is no store).
  private val initInvocations = InvocationCounter[ProjectRef]()

  private val entityType = EntityType("test")

  private def success[A](entityType: EntityType, project: ProjectRef, id: Iri, value: A, offset: Long): SuccessElem[A] =
    SuccessElem(tpe = entityType, id, project, Instant.EPOCH, Offset.at(offset), value, 1)

  private def projectElem(project: ProjectRef, markedForDeletion: Boolean, offset: Long): SuccessElem[ProjectDef] =
    success(Projects.entityType, project, Projects.encodeId(project), ProjectDef(project, markedForDeletion), offset)

  // Streams 3 project states (incl. an inactive project) until the signal is set, then re-emits project1 and marks
  // project2 for deletion. Trailing `Stream.never` keeps the coordinator alive so the `handleActivations` subscriber
  // (registered via `concurrently`) stays alive for the resume test.
  private def projectStream: SuccessElemStream[ProjectDef] =
    Stream(
      projectElem(project1, markedForDeletion = false, 1L),
      projectElem(project2, markedForDeletion = false, 2L),
      projectElem(project3, markedForDeletion = false, 3L)
    ) ++ Stream.never[IO].interruptWhen(resumeSignal) ++
      Stream(
        projectElem(project1, markedForDeletion = false, 4L),
        projectElem(project2, markedForDeletion = true, 5L)
      ) ++ Stream.never[IO]

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

  private val projectProjectionFactory = new ProjectProjectionLifecycle {
    override def module: String = "main-indexing"

    override def bootstrap: IO[Unit] = IO.unit

    override def name(project: ProjectRef): String = projectionName(project)

    override def onInit(project: ProjectRef): IO[Unit] = initInvocations.increment(project)

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

  // Completed projections are evicted from supervision; persisted progress is the canonical signal.
  private def assertCompleted(project: ProjectRef)(using Location) =
    projections.progress(projectionName(project)).assertEquals(Some(expectedProgress)).eventually

  private def assertCoordinatorProgress(expected: ProjectionProgress)(using Location) =
    sv.describe(ProjectDefCoordinator.metadata.name).map(_.map(_.progress)).assertEquals(Some(expected)).eventually

  // A resumable projection must be absent from supervision before an activation can (re)start it: `supervisor.run` is
  // idempotent, so publishing while it is still running would be a no-op and would not re-run `onInit`.
  private def assertEvicted(project: ProjectRef)(using Location) =
    sv.describe(projectionName(project)).assertEquals(None).eventually

  test("Start the coordinator") {
    ProjectDefCoordinator(sv, _ => projectStream, resumer, Set(projectProjectionFactory)) >>
      assertCoordinatorProgress(ProjectionProgress(Offset.at(3L), Instant.EPOCH, 3, 0, 0))
  }

  test(s"Projection for '$project1' processed all items and completed") {
    assertCompleted(project1)
  }

  test(s"Projection for '$project2' processed all items and completed too") {
    assertCompleted(project2)
  }

  test(s"Projection for the inactive '$project3' should not be started") {
    projections.progress(projectionName(project3)).assertEquals(None) >>
      initInvocations.count(project3).assertEquals(0)
  }

  test("Resume the stream of projects") {
    resumeSignal.set(true) >>
      assertCoordinatorProgress(ProjectionProgress(Offset.at(5L), Instant.EPOCH, 5, 0, 0))
  }

  test(s"Projection for '$project1' should not be restarted by the new project state") {
    // The previous projection completed and was evicted; the re-emitted state must not change its persisted progress.
    projections.progress(projectionName(project1)).assertEquals(Some(expectedProgress))
  }

  test(s"Publishing a project activation resumes the evicted '$project1' projection") {
    for {
      _      <- assertEvicted(project1)
      before <- initInvocations.count(project1)
      _      <- activations.publish(ProjectionActivation.ForProject(project1))
      _      <- initInvocations.count(project1).map(_ > before).assertEquals(true).eventually
    } yield ()
  }

  test(s"Publishing a single-projection activation for the factory's module resumes '$project1'") {
    val metadata =
      ProjectionMetadata("main-indexing", projectionName(project1), Some(project1), Some(nxv + "projection"))
    for {
      _      <- assertEvicted(project1)
      before <- initInvocations.count(project1)
      _      <- activations.publish(ProjectionActivation.ForProjection(metadata))
      _      <- initInvocations.count(project1).map(_ > before).assertEquals(true).eventually
    } yield ()
  }

  test("A single-projection activation for another module is ignored") {
    val otherModule = ProjectionMetadata("elasticsearch", "es-view", Some(project1), Some(nxv + "view"))
    val sameModule  =
      ProjectionMetadata("main-indexing", projectionName(project1), Some(project1), Some(nxv + "projection"))
    for {
      _      <- assertEvicted(project1)
      before <- initInvocations.count(project1)
      // The other-module activation must be ignored. Publishing it before a matching one means that, once the matching
      // resume is observed, the ignored one has already been processed (single ordered consumer), so the count rose by
      // exactly one — proving the other-module activation resumed nothing.
      _      <- activations.publish(ProjectionActivation.ForProjection(otherModule))
      _      <- activations.publish(ProjectionActivation.ForProjection(sameModule))
      _      <- initInvocations.count(project1).assertEquals(before + 1).eventually
    } yield ()
  }

  test(s"'$project2' is marked for deletion, the associated projection should be destroyed") {
    projections.progress(projectionName(project2)).assertEquals(None)
  }

}
