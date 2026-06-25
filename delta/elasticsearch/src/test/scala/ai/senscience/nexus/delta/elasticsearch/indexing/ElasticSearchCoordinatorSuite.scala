package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.indexing.ElasticSearchRunningStore.ElasticRunningView
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchViews, Fixtures}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.SupervisorSetup.unapply
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.circe.Json
import munit.AnyFixture

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

class ElasticSearchCoordinatorSuite extends NexusSuite with SupervisorSetup.Fixture with Fixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(supervisor)

  private given PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val indexingRev = IndexingRev.init
  private val rev         = 2
  private val uuid        = UUID.randomUUID()

  private lazy val (sv, projections, projectionErrors) = unapply(supervisor())
  private val project                                  = ProjectRef.unsafe("org", "proj")
  private val indexDef                                 = ElasticsearchIndexDef.empty

  private def activeView(
      ref: ViewRef,
      index: String,
      revision: IndexingRev = indexingRev,
      pipeChain: Option[PipeChain] = None
  ): ActiveViewDef =
    ActiveViewDef(ref, pipeChain, SelectFilter.latest, IndexLabel.unsafe(index), indexDef, None, revision, rev, uuid)

  private val id1   = nxv + "view1"
  private val view1 = activeView(ViewRef(project, id1), "view1")

  private val id2   = nxv + "view2"
  private val view2 = activeView(ViewRef(project, id2), "view2")

  private val id3         = nxv + "view3"
  private val unknownPipe = PipeRef.unsafe("xxx")
  private val view3       =
    activeView(ViewRef(project, id3), "view3", pipeChain = Some(PipeChain(unknownPipe -> ExpandedJsonLd.empty)))

  private val deprecatedView1 = DeprecatedViewDef(
    ViewRef(project, id1)
  )
  // A reindex bumps the indexing revision (keeping the same view uuid); this drives the projection name and index.
  private val updatedRev      = IndexingRev(2)
  private val updatedView2    = activeView(ViewRef(project, id2), "view2_2", revision = updatedRev)

  private val resumeSignal = SignallingRef[IO, Boolean](false).unsafeRunSync()
  private val resumeStream = Stream.never[IO].interruptWhen(resumeSignal)

  private def viewAsElem(view: IndexingViewDef, offset: Long): SuccessElem[IndexingViewDef] =
    SuccessElem(
      tpe = ElasticSearchViews.entityType,
      id = view.ref.viewId,
      project = view.ref.project,
      instant = Instant.EPOCH,
      offset = Offset.at(offset),
      value = view,
      rev = 1
    )

  // Streams 3 views until the signal is set, then 1 deprecated and 1 updated view
  private def viewStream: SuccessElemStream[IndexingViewDef] =
    Stream(viewAsElem(view1, 1L), viewAsElem(view2, 2L), viewAsElem(view3, 3L)) ++
      resumeStream ++
      Stream(
        viewAsElem(deprecatedView1, 5L),
        viewAsElem(updatedView2, 6L),
        // Offset 7 represents a view update that does not require reindexing
        viewAsElem(updatedView2, 7L)
      ) ++ Stream.never[IO]

  private val expectedViewProgress: ProjectionProgress = ProjectionProgress(
    Offset.at(4L),
    Instant.EPOCH,
    processed = 4,
    discarded = 1,
    failed = 1
  )

  private val graphStream = GraphResourceStream.unsafeFromStream(PullRequestStream.generate(project))
  // The running store the test lifecycle records into; the coordinator no longer sees it directly. We assert init/destroy
  // ran via the store contents (a recorded row means init ran; its absence means it was never started or was destroyed).
  private val store       = ElasticSearchRunningStore.inMemory
  private val lifecycle   = new ElasticProjectionLifecycle {
    override def compile(view: ActiveViewDef): IO[CompiledProjection] =
      IndexingViewDef.compile(view, PipeChainCompiler.alwaysFail, graphStream, new NoopSink[Json])
    override def init(view: ActiveViewDef): IO[Unit]                  =
      store.save(ElasticRunningView(view.ref, view.indexingRev, view.uuid))
    override def recorded(ref: ViewRef): IO[List[ElasticRunningView]] =
      store.list(ref)
    override def destroy(view: ElasticRunningView): IO[Unit]          =
      store.delete(view.ref, view.indexingRev)
  }

  private def isRecorded(view: ActiveViewDef): IO[Boolean] =
    store.list(view.ref).map(_.contains(ElasticRunningView(view.ref, view.indexingRev, view.uuid)))

  private def assertRecorded(view: ActiveViewDef)(using munit.Location): IO[Unit]    = isRecorded(view).assertEquals(true)
  private def assertNotRecorded(view: ActiveViewDef)(using munit.Location): IO[Unit] =
    isRecorded(view).assertEquals(false)

  private def fetchIndexingErrors(metadata: ProjectionMetadata) =
    projectionErrors.failedElemEntries(metadata.name, Offset.start).compile.toList

  private def fetchCoordinatorProgress =
    sv.describe(ElasticSearchCoordinator.metadata.name).map(_.map(_.progress))

  // Tracks that the coordinator started the resume stream.
  private val resumerStarted                    = Ref.unsafe[IO, Boolean](false)
  private val resumer: ElasticProjectionResumer = new ProjectionResumer[ActiveViewDef] {
    override def run(resume: ActiveViewDef => IO[Unit]): Stream[IO, Unit] =
      Stream.exec(resumerStarted.set(true)) ++ Stream.never[IO]
  }

  test("Start the coordinator") {
    val expectedProgress = ProjectionProgress(Offset.at(3L), Instant.EPOCH, 3, 0, 1)
    ElasticSearchCoordinator((_: Offset) => viewStream, lifecycle, sv, resumer) >>
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

  test("There is one error for the coordinator projection before the signal") {
    fetchIndexingErrors(ElasticSearchCoordinator.metadata).map { entries =>
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
    val expectedProgress = ProjectionProgress(Offset.at(7L), Instant.EPOCH, 6, 0, 1)
    resumeSignal.set(true) >>
      fetchCoordinatorProgress.assertEquals(Some(expectedProgress)).eventually
  }

  test("View 1 is deprecated so it is stopped, the progress and the running record should be deleted.") {
    projections.progress(view1.projection).assertEquals(None) >>
      assertNotRecorded(view1)
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

  test("Updated view 2 is still recorded as the offset 6 update did not require a restart") {
    assertRecorded(updatedView2)
  }

}
