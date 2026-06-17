package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.plugins.compositeviews.{CompositeViews, CompositeViewsFixture}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.SupervisorSetup.unapply
import ai.senscience.nexus.testkit.InvocationCounter
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import fs2.Stream
import fs2.concurrent.SignallingRef
import munit.AnyFixture

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

class CompositeViewsCoordinatorSuite extends NexusSuite with SupervisorSetup.Fixture with CompositeViewsFixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(supervisor)

  private given PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private lazy val (sv, _, _) = unapply(supervisor())

  private def activeView(ref: ViewRef) = ActiveViewDef(ref, UUID.randomUUID(), 1, viewValue)

  private val view1        = activeView(ViewRef(projectRef, nxv + "view1"))
  private val view2        = activeView(ViewRef(projectRef, nxv + "view2"))
  // A view whose project is inactive: the coordinator should not start it.
  private val inactiveView = activeView(ViewRef(otherProjectRef, nxv + "view3"))
  private val deprecated1  = DeprecatedViewDef(view1.ref)

  private val resumeSignal = SignallingRef[IO, Boolean](false).unsafeRunSync()

  private def viewAsElem(view: CompositeViewDef, offset: Long): SuccessElem[CompositeViewDef] =
    SuccessElem(CompositeViews.entityType, view.ref.viewId, view.ref.project, Instant.EPOCH, Offset.at(offset), view, 1)

  // Streams 3 views (incl. one on an inactive project) until the signal is set, then deprecates view1.
  private def fetchViews: Stream[IO, SuccessElem[CompositeViewDef]] =
    Stream(viewAsElem(view1, 1L), viewAsElem(view2, 2L), viewAsElem(inactiveView, 3L)) ++
      Stream.never[IO].interruptWhen(resumeSignal) ++
      Stream(viewAsElem(deprecated1, 4L)) ++ Stream.never[IO]

  // Test lifecycle: builds a trivial (empty, completing) projection and records init/destroy invocations in-memory.
  private val started   = InvocationCounter[ViewRef]()
  private val destroyed = Ref.unsafe[IO, Set[ViewRef]](Set.empty)
  private val lifecycle = new CompositeProjectionLifeCycle {
    override def init(view: ActiveViewDef): IO[Unit]                                            =
      started.increment(view.ref)
    override def build(view: ActiveViewDef): IO[CompiledProjection]                             =
      IO.pure(
        CompiledProjection.fromStream(view.metadata, ExecutionStrategy.PersistentSingleNode, _ => Stream.empty[IO])
      )
    override def destroyOnIndexingChange(prev: ActiveViewDef, next: CompositeViewDef): IO[Unit] =
      destroyed.update(_ + prev.ref)
  }

  private def coordinatorProgress =
    sv.describe(CompositeViewsCoordinator.metadata.name).map(_.map(_.progress))

  test("Start the coordinator, run the resume stream and start the active views") {
    CompositeViewsCoordinator((_: Offset) => fetchViews, sv, lifecycle) >>
      coordinatorProgress.assertEquals(Some(ProjectionProgress(Offset.at(3L), Instant.EPOCH, 3, 0, 0))).eventually >>
      started.count(view1.ref).map(_ > 0).assertEquals(true).eventually >>
      started.count(view2.ref).map(_ > 0).assertEquals(true).eventually
  }

  test("Resume the stream of views, deprecating view1") {
    resumeSignal.set(true) >>
      coordinatorProgress.assertEquals(Some(ProjectionProgress(Offset.at(4L), Instant.EPOCH, 4, 0, 0))).eventually
  }

  test("Deprecating view1 cleans up its projection") {
    destroyed.get.map(_.contains(view1.ref)).assertEquals(true)
  }

}
