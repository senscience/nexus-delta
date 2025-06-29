package ai.senscience.nexus.delta.plugins.compositeviews.projections

import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeRestart.FullRestart
import ai.senscience.nexus.delta.plugins.compositeviews.store.{CompositeProgressStore, CompositeRestartStore}
import ai.senscience.nexus.delta.plugins.compositeviews.stream.CompositeBranch.Run
import ai.senscience.nexus.delta.plugins.compositeviews.stream.{CompositeBranch, CompositeProgress}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, IndexingViewRef, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.stream.ProjectionProgress
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import fs2.Stream
import munit.AnyFixture

import java.time.Instant
import scala.concurrent.duration.*

class CompositeProjectionsSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  implicit private val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 100.millis)

  private lazy val compositeRestartStore  = new CompositeRestartStore(xas)
  private lazy val compositeProgressStore = new CompositeProgressStore(xas, clock)
  private lazy val projections            =
    CompositeProjections(compositeRestartStore, xas, queryConfig, BatchConfig(10, 50.millis), 3.seconds, clock)

  private val project     = ProjectRef.unsafe("org", "proj")
  private val viewRef     = ViewRef(project, nxv + "id")
  private val indexingRev = IndexingRev(2)
  private val view        = IndexingViewRef(viewRef, indexingRev)

  private val source  = nxv + "source"
  private val target1 = nxv + "target1"
  private val target2 = nxv + "target2"

  private val mainBranch1   = CompositeBranch(source, target1, Run.Main)
  private val mainProgress1 = ProjectionProgress(Offset.At(42L), Instant.EPOCH, 5, 2, 1)

  private val mainBranch2   = CompositeBranch(source, target2, Run.Main)
  private val mainProgress2 = ProjectionProgress(Offset.At(22L), Instant.EPOCH, 2, 1, 0)

  private val viewRef2      = ViewRef(project, nxv + "id2")
  private val view2         = IndexingViewRef(viewRef2, indexingRev)
  private val view2Progress = ProjectionProgress(Offset.At(999L), Instant.EPOCH, 514, 140, 0)

  // Check that view 2 is not affected by changes on view 1
  private def assertView2 = {
    val expected = Map(mainBranch1 -> view2Progress)
    compositeProgressStore.progress(view2).assertEquals(expected)
  }

  // Save progress for view 1
  private def saveView1 =
    for {
      _ <- compositeProgressStore.save(view, mainBranch1, mainProgress1)
      _ <- compositeProgressStore.save(view, mainBranch2, mainProgress2)
    } yield ()

  test("Save progress for all branches and views") {
    for {
      _ <- saveView1
      _ <- compositeProgressStore.save(view2, mainBranch1, view2Progress)
    } yield ()
  }

  test("Return new progress") {
    val expected = CompositeProgress(
      Map(mainBranch1 -> mainProgress1, mainBranch2 -> mainProgress2)
    )

    for {
      _ <- projections.progress(view).assertEquals(expected)
      _ <- assertView2
    } yield ()
  }

  test("Save a composite restart and reset progress") {
    val restart = FullRestart(viewRef, Instant.EPOCH, Anonymous)
    for {
      value   <- Ref.of[IO, Int](0)
      inc      = Stream.eval(value.getAndUpdate(_ + 1)) ++ Stream.never[IO]
      _       <- inc.through(projections.handleRestarts(viewRef)).compile.drain.start
      _       <- value.get.assertEquals(1).eventually
      _       <- compositeRestartStore.save(restart)
      expected = CompositeProgress(
                   Map(mainBranch1 -> ProjectionProgress.NoProgress, mainBranch2 -> ProjectionProgress.NoProgress)
                 )
      _       <- projections.progress(view).assertEquals(expected).eventually
      _       <- compositeRestartStore.head(viewRef).assertEquals(None)
      _       <- value.get.assertEquals(2).eventually
      _       <- assertView2
    } yield ()
  }

  test("Delete all progress") {
    for {
      _ <- projections.deleteAll(view)
      _ <- projections.progress(view).assertEquals(CompositeProgress(Map.empty))
    } yield ()

  }
}
