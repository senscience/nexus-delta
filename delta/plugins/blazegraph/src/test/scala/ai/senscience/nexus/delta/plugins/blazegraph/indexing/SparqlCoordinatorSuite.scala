package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.SparqlRunningStore.SparqlRunningView
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.ProjectionErr.CouldNotFindPipeErr
import ai.senscience.nexus.delta.sourcing.stream.SupervisorSetup.unapply
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import fs2.Stream
import fs2.concurrent.SignallingRef
import munit.AnyFixture

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

class SparqlCoordinatorSuite extends NexusSuite with SupervisorSetup.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(supervisor)

  private given PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val indexingRev = 1
  private val rev         = 2
  private val uuid        = UUID.randomUUID()

  private lazy val (sv, projections, projectionErrors) = unapply(supervisor())
  private val project                                  = ProjectRef.unsafe("org", "proj")

  private def activeView(
      ref: ViewRef,
      namespace: String,
      revision: Int = indexingRev,
      pipeChain: Option[PipeChain] = None
  ): ActiveViewDef =
    ActiveViewDef(ref, SelectFilter.latest, pipeChain, namespace, revision, rev, uuid)

  private val id1   = nxv + "view1"
  private val view1 = activeView(ViewRef(project, id1), "view1")

  private val id2   = nxv + "view2"
  private val view2 = activeView(ViewRef(project, id2), "view2")

  private val id3              = nxv + "view3"
  private val unknownPipe      = PipeRef.unsafe("xxx")
  private val failingPipeChain = PipeChain(unknownPipe -> ExpandedJsonLd.empty)
  private val view3            = activeView(ViewRef(project, id3), "view3", pipeChain = Some(failingPipeChain))

  // A view whose project is inactive: the coordinator should not start it.
  private val inactiveProject = ProjectRef.unsafe("org", "inactive")
  private val id4             = nxv + "view4"
  private val inactiveView    = activeView(ViewRef(inactiveProject, id4), "view4")

  private val deprecatedView1   = DeprecatedViewDef(ViewRef(project, id1), uuid, indexingRev)
  // A deprecated view that was never recorded in the store (as the immutable default view never is): cleanup must
  // still run from the base values carried by the def.
  private val deprecatedDefault =
    DeprecatedViewDef(ViewRef(project, nxv + "default-like"), UUID.randomUUID(), indexingRev)
  // A reindex bumps the indexing revision (keeping the same view uuid); this drives the projection name and namespace.
  private val updatedRev        = 2
  private val updatedView2      = activeView(ViewRef(project, id2), "view2_2", revision = updatedRev)

  private val resumeSignal = SignallingRef[IO, Boolean](false).unsafeRunSync()
  private val resumeStream = Stream.never[IO].interruptWhen(resumeSignal)

  private def viewAsElem(view: IndexingViewDef, offset: Long): SuccessElem[IndexingViewDef] =
    SuccessElem(
      tpe = BlazegraphViews.entityType,
      id = view.ref.viewId,
      project = view.ref.project,
      instant = Instant.EPOCH,
      offset = Offset.at(offset),
      value = view,
      rev = 1
    )

  // Streams 4 elements (incl. a view on an inactive project) until the signal is set, then 1 deprecated and 1 updated view
  private def fetchViews: SuccessElemStream[IndexingViewDef] =
    Stream(viewAsElem(view1, 1L), viewAsElem(view2, 2L), viewAsElem(view3, 3L), viewAsElem(inactiveView, 4L)) ++
      resumeStream ++
      Stream(
        viewAsElem(deprecatedView1, 5L),
        viewAsElem(deprecatedDefault, 6L),
        viewAsElem(updatedView2, 7L),
        // Offset 8 represents a view update that does not require reindexing
        viewAsElem(updatedView2, 8L)
      ) ++ Stream.never[IO]

  private val expectedViewProgress: ProjectionProgress = ProjectionProgress(
    Offset.at(4L),
    Instant.EPOCH,
    processed = 4,
    discarded = 1,
    failed = 1
  )

  // The running store the test lifecycle records into; the coordinator no longer sees it directly. We assert init/destroy
  // ran via the store contents (a recorded row means init ran; its absence means it was never started or was destroyed).
  private val store     = SparqlRunningStore.inMemory
  // Records every running view passed to destroy, so we can assert cleanup even for views that were never recorded
  // (where `store.delete` is a no-op and thus invisible in the store).
  private val destroyed = Ref.unsafe[IO, Set[SparqlRunningView]](Set.empty)
  private val lifecycle = new SparqlProjectionLifeCycle {
    override def compile(view: ActiveViewDef): IO[CompiledProjection] =
      IndexingViewDef.compile(
        view,
        (_: PipeChain) => Left(CouldNotFindPipeErr(unknownPipe)),
        PullRequestStream.generate(project),
        new NoopSink[NTriples]
      )
    override def init(view: ActiveViewDef): IO[Unit]                  =
      store.save(SparqlRunningView(view.ref, view.indexingRev, view.uuid))
    override def recorded(ref: ViewRef): IO[List[SparqlRunningView]]  =
      store.list(ref)
    override def destroy(view: SparqlRunningView): IO[Unit]           =
      store.delete(view.ref, view.indexingRev) >> destroyed.update(_ + view)
  }

  private def isRecorded(view: ActiveViewDef): IO[Boolean] =
    store.list(view.ref).map(_.contains(SparqlRunningView(view.ref, view.indexingRev, view.uuid)))

  private def assertRecorded(view: ActiveViewDef)(using munit.Location): IO[Unit]    = isRecorded(view).assertEquals(true)
  private def assertNotRecorded(view: ActiveViewDef)(using munit.Location): IO[Unit] =
    isRecorded(view).assertEquals(false)

  private def fetchIndexingErrors(metadata: ProjectionMetadata) =
    projectionErrors.failedElemEntries(metadata.name, Offset.start).compile.toList

  private def fetchCoordinatorProgress =
    sv.describe(SparqlCoordinator.metadata.name).map(_.map(_.progress))

  // Tracks that the coordinator started the resume stream; `isActive` only treats `project` as active.
  private val resumerStarted                   = Ref.unsafe[IO, Boolean](false)
  private val resumer: SparqlProjectionResumer = new ProjectionResumer[ActiveViewDef] {
    override def isActive(p: ProjectRef): IO[Boolean]                     = IO.pure(p == project)
    override def run(resume: ActiveViewDef => IO[Unit]): Stream[IO, Unit] =
      Stream.exec(resumerStarted.set(true)) ++ Stream.never[IO]
  }

  test("Start the coordinator") {
    val expectedProgress = ProjectionProgress(Offset.at(4L), Instant.EPOCH, 4, 0, 1)
    SparqlCoordinator((_: Offset) => fetchViews, lifecycle, sv, resumer) >>
      resumerStarted.get.assertEquals(true).eventually >>
      fetchCoordinatorProgress.assertEquals(Some(expectedProgress)).eventually
  }

  test("View 1 processed all items and completed") {
    projections.progress(view1.projection).assertEquals(Some(expectedViewProgress)).eventually >>
      assertRecorded(view1)
  }

  test("View 2 processed all items and completed too") {
    projections.progress(view2.projection).assertEquals(Some(expectedViewProgress)).eventually >>
      assertRecorded(view2)
  }

  test("View 3 is invalid so it should not be started") {
    projections.progress(view3.projection).assertEquals(None) >>
      assertNotRecorded(view3)
  }

  test("The view on an inactive project should not be started") {
    projections.progress(inactiveView.projection).assertEquals(None) >>
      assertNotRecorded(inactiveView)
  }

  test("There is one error for the coordinator projection before the signal") {
    fetchIndexingErrors(SparqlCoordinator.metadata).map { entries =>
      val r = entries.assertOneElem
      assertEquals(r.failedElemData.id, id3)
    }
  }

  test("There is one error for view 1") {
    fetchIndexingErrors(view1.projectionMetadata).map { entries =>
      val r = entries.assertOneElem
      assertEquals(r.failedElemData.id, nxv + "failed")
      assertEquals(r.failedElemData.entityType, PullRequest.entityType)
      assertEquals(r.failedElemData.offset, Offset.At(4))
    }
  }

  test("There is one error for view 2") {
    fetchIndexingErrors(view2.projectionMetadata).map(_.assertOneElem)
  }

  test("There are no errors for view 3") {
    fetchIndexingErrors(view3.projectionMetadata).map(_.assertEmpty())
  }

  test("Resume the stream of view") {
    val expectedProgress = ProjectionProgress(Offset.at(8L), Instant.EPOCH, 8, 0, 1)
    resumeSignal.set(true) >>
      fetchCoordinatorProgress.assertEquals(Some(expectedProgress)).eventually
  }

  test("View 1 is deprecated so it is stopped, the progress and the running record should be deleted.") {
    projections.progress(view1.projection).assertEquals(None) >>
      assertNotRecorded(view1)
  }

  test("A deprecated view that was never recorded (the default view) is still cleaned up via the def's base values") {
    val expected = SparqlRunningView(deprecatedDefault.ref, deprecatedDefault.indexingRev, deprecatedDefault.uuid)
    destroyed.get.map(_.contains(expected)).assertEquals(true).eventually
  }

  test(
    "View 2 is updated so the previous projection should be stopped, the previous progress and running record deleted."
  ) {
    projections.progress(view2.projection).assertEquals(None) >>
      assertNotRecorded(view2)
  }

  test("Updated view 2 processed all items and completed") {
    projections.progress(updatedView2.projection).assertEquals(Some(expectedViewProgress)).eventually >>
      assertRecorded(updatedView2)
  }

  test("View 2_2 projection should have one error after failed elem offset 4") {
    fetchIndexingErrors(updatedView2.projectionMetadata).map { entries =>
      val r = entries.assertOneElem
      assertEquals(r.failedElemData.id, nxv + "failed")
    }
  }

  test("Updated view 2 is still recorded as the offset 7 update did not require a restart") {
    assertRecorded(updatedView2)
  }

}
