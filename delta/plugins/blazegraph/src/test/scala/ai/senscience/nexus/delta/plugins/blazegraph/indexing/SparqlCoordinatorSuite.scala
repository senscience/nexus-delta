package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
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
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.collection.mutable.Set as MutableSet
import scala.concurrent.duration.*

class SparqlCoordinatorSuite extends NexusSuite with SupervisorSetup.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(supervisor)

  private given ProjectionBackpressure = ProjectionBackpressure.Noop
  private given PatienceConfig         = PatienceConfig(5.seconds, 10.millis)

  private val indexingRev = 1
  private val rev         = 2

  private lazy val (sv, projections, projectionErrors) = unapply(supervisor())
  private val project                                  = ProjectRef.unsafe("org", "proj")
  private val id1                                      = nxv + "view1"
  private val view1                                    = ActiveViewDef(
    ViewRef(project, id1),
    projection = id1.toString,
    SelectFilter.latest,
    None,
    namespace = "view1",
    indexingRev,
    rev
  )

  private val id2   = nxv + "view2"
  private val view2 = ActiveViewDef(
    ViewRef(project, id2),
    projection = id2.toString,
    SelectFilter.latest,
    None,
    namespace = "view2",
    indexingRev,
    rev
  )

  private val id3              = nxv + "view3"
  private val unknownPipe      = PipeRef.unsafe("xxx")
  private val failingPipeChain = PipeChain(unknownPipe -> ExpandedJsonLd.empty)
  private val view3            = ActiveViewDef(
    ViewRef(project, id3),
    projection = id3.toString,
    SelectFilter.latest,
    Some(failingPipeChain),
    namespace = "view3",
    indexingRev,
    rev
  )

  private val deprecatedView1 = DeprecatedViewDef(
    ViewRef(project, id1)
  )
  private val updatedView2    = ActiveViewDef(
    ViewRef(project, id2),
    projection = id2.toString + "_2",
    SelectFilter.latest,
    None,
    namespace = "view2_2",
    indexingRev,
    rev
  )

  private val pausingRef = Ref.of[IO, Boolean](false).unsafeRunSync()

  private def viewAsElem(view: IndexingViewDef, offset: Long) =
    SuccessElem(
      tpe = BlazegraphViews.entityType,
      id = view.ref.viewId,
      project = project,
      instant = Instant.EPOCH,
      offset = Offset.at(offset),
      value = view,
      rev = 1
    )

  private def sleep = Stream.sleep[IO](100.millis).drain

  // Streams 3 elements until pausingRef is set to true, then 1 updated view and 1 deprecated view
  private def fetchViews: SuccessElemStream[IndexingViewDef] =
    Stream(
      viewAsElem(view1, 1L),
      viewAsElem(view2, 2L),
      viewAsElem(view3, 3L)
    ) ++ sleep ++ Stream.exec(pausingRef.set(true)) ++ sleep ++
      Stream(
        viewAsElem(deprecatedView1, 4L),
        viewAsElem(updatedView2, 5L),
        // Elem at offset 6 represents a view update that does not require reindexing
        viewAsElem(updatedView2, 6L)
      )

  private val createdIndices                           = MutableSet.empty[String]
  private val deletedIndices                           = MutableSet.empty[String]
  private val expectedViewProgress: ProjectionProgress = ProjectionProgress(
    Offset.at(4L),
    Instant.EPOCH,
    processed = 4,
    discarded = 1,
    failed = 1
  )

  private val projectionCycle = new SparqlProjectionLifeCycle {

    override def compile(view: ActiveViewDef): IO[CompiledProjection] =
      IndexingViewDef.compile(
        view,
        (_: PipeChain) => Left(CouldNotFindPipeErr(unknownPipe)),
        PullRequestStream.generate(project),
        new NoopSink[NTriples]
      )

    override def init(view: ActiveViewDef): IO[Unit] = IO.delay(createdIndices.add(view.namespace)).void

    override def destroy(view: ActiveViewDef): IO[Unit] = IO.delay(deletedIndices.add(view.namespace)).void
  }

  private def assertSupervisorProgress(projection: String, expectedProgress: ProjectionProgress) =
    sv.describe(projection)
      .map(_.map(_.progress))
      .assertEquals(Some(expectedProgress))
      .eventually

  private def assertViewProgress(view: ActiveViewDef, expectedProgress: ProjectionProgress) =
    projections.progress(view.projection).assertEquals(Some(expectedProgress))

  private def assertViewNoProgress(view: ActiveViewDef) =
    projections.progress(view.projection).assertEquals(None)

  private def assertViewCompleted(view: ActiveViewDef)(implicit location: Location) =
    sv.describe(view.projection)
      .map(_.map(_.status))
      .assertEquals(Some(ExecutionStatus.Completed))
      .eventually

  private def assertNoRunningView(view: ActiveViewDef)(implicit location: Location) =
    sv.describe(view.projection)
      .assertEquals(None)
      .eventually

  private def assertNamespaceCreated(view: ActiveViewDef)(implicit location: Location): Unit =
    assert(
      createdIndices.contains(view.namespace),
      s"The index for '${view.ref.viewId}' should have been created."
    )

  private def assertNamespaceDeleted(view: ActiveViewDef)(implicit location: Location): Unit =
    assert(
      deletedIndices.contains(view.namespace),
      s"The index for '${view.ref.viewId}' should have been deleted."
    )

  private def fetchProjectionErrors(projection: String) =
    projectionErrors.failedElemEntries(projection, Offset.start).compile.toList

  test("Start the coordinator") {
    for {
      _ <- SparqlCoordinator((_: Offset) => fetchViews, projectionCycle, sv)
      _ <- assertSupervisorProgress(
             SparqlCoordinator.metadata.name,
             ProjectionProgress(Offset.at(3L), Instant.EPOCH, 3, 0, 1)
           )
      // There is one error for the coordinator projection before the signal
      _ <- fetchProjectionErrors(SparqlCoordinator.metadata.name).map { entries =>
             val r = entries.assertOneElem
             assertEquals(r.failedElemData.id, id3)
           }.eventually
    } yield ()
  }

  test("View 1 processed all items and completed") {
    for {
      _ <- assertViewCompleted(view1)
      _ <- assertViewProgress(view1, expectedViewProgress)
      _  = assertNamespaceCreated(view1)
      _ <- fetchProjectionErrors(view1.projection).map { entries =>
             val r = entries.assertOneElem
             assertEquals(r.failedElemData.id, nxv + "failed")
             assertEquals(r.failedElemData.entityType, PullRequest.entityType)
             assertEquals(r.failedElemData.offset, Offset.At(4))
           }
    } yield ()
  }

  test("View 2 processed all items and completed too") {
    for {
      _ <- assertViewCompleted(view2)
      _ <- assertViewProgress(view2, expectedViewProgress)
      _  = assertNamespaceCreated(view2)
      _ <- fetchProjectionErrors(view2.projection).map(_.assertSize(1))
    } yield ()
  }

  test("View 3 is invalid so it should not be started") {
    for {
      _ <- assertNoRunningView(view3)
      _ <- assertViewNoProgress(view3)
      _  = assert(
             !createdIndices.contains(view3.namespace),
             s"The index for '${view3.ref.viewId}' should not have been created."
           )
      _ <- fetchProjectionErrors(view3.projection).map(_.assertEmpty())
    } yield ()
  }

  test("Resume the stream of view") {
    for {
      _ <- pausingRef.set(false)
      _ <- assertSupervisorProgress(
             SparqlCoordinator.metadata.name,
             ProjectionProgress(Offset.at(6L), Instant.EPOCH, 6, 0, 1)
           )
    } yield ()
  }

  test("View 1 is deprecated so it is stopped, the progress and the index should be deleted.") {
    for {
      _ <- assertNoRunningView(view1)
      _ <- assertViewNoProgress(view1)
      _  = assertNamespaceDeleted(view1)
    } yield ()
  }

  test("View 2 is updated so the previous projection should be stopped and its progress/index should be deleted.") {
    for {
      _ <- assertNoRunningView(view2)
      _ <- assertViewNoProgress(view2)
      _  = assertNamespaceDeleted(view2)
    } yield ()
  }

  test("Updated view 2 processed all items and completed") {
    for {
      _ <- assertViewCompleted(updatedView2)
      _ <- assertViewProgress(updatedView2, expectedViewProgress)
      _  = assertNamespaceCreated(updatedView2)
      _ <- fetchProjectionErrors(updatedView2.projection).map { entries =>
             val r = entries.assertOneElem
             assertEquals(r.failedElemData.id, nxv + "failed")
           }
      // Delete indices should not contain view2_2 as it was not restarted
      _  = assert(!deletedIndices.contains(updatedView2.projection))
    } yield ()
  }

}
