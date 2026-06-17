package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.ExecutionStatus.Running
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectionTermination
import ai.senscience.nexus.delta.sourcing.stream.ExecutionStrategy.{EveryNode, PersistentSingleNode, TransientSingleNode}
import ai.senscience.nexus.delta.sourcing.stream.SupervisorSuite.UnstableDestroy
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.*

class SupervisorSuite extends NexusSuite with SupervisorSetup.Fixture with Doobie.Assertions {

  private given PatienceConfig = PatienceConfig(1.second, 50.millis)
  private given Subject        = Anonymous

  override def munitFixtures: Seq[AnyFixture[?]] = List(supervisor3_1)

  private lazy val setup       = supervisor3_1()
  private lazy val sv          = setup.supervisor
  private lazy val projections = setup.projections
  private lazy val terminalLog = setup.terminalLog
  // name1 should run on the node with index 1 in a 3-node cluster
  private val runnableByNode1  = ProjectionMetadata("test", "name1", None, None)
  // name2 should NOT run on the node with index 1 of a 3-node cluster
  private val ignoredByNode1   = ProjectionMetadata("test", "name2", None, None)
  private val random           = ProjectionMetadata("test", "name3", None, None)

  private val project = ProjectRef.unsafe("org", "proj")

  private val rev = 1

  private def evalStream(start: IO[Unit]): Offset => ElemStream[Unit] =
    (_: Offset) =>
      Stream.eval(start) >> Stream
        .range(1, 21)
        .map { value =>
          SuccessElem(EntityType("entity"), nxv + "id", project, Instant.EPOCH, Offset.at(value.toLong), (), rev)
        }

  private val expectedProgress = ProjectionProgress(Offset.at(20L), Instant.EPOCH, 20, 0, 0)

  private def startProjection(metadata: ProjectionMetadata, strategy: ExecutionStrategy)(using Location) =
    for {
      started <- Ref.of[IO, Boolean](false)
      compiled = CompiledProjection.fromStream(metadata, strategy, evalStream(started.set(true)))
      _       <- sv.run(compiled)
      _       <- started.get.assertEquals(true).eventually
    } yield ()

  private def assertInitCrash(metadata: ProjectionMetadata, strategy: ExecutionStrategy)(using Location) = {
    val expectedException = new IllegalStateException("The init task crashed unexpectedly.")
    for {
      started       <- Ref.of[IO, Boolean](false)
      alreadyFailed <- Ref.of[IO, Boolean](false)
      initFailedOnce = alreadyFailed.get.flatMap {
                         case true  => IO.unit
                         case false => alreadyFailed.set(true) >> IO.raiseError(expectedException)
                       }
      compiled       = CompiledProjection.fromStream(metadata, strategy, evalStream(started.set(true)))
      _             <- sv.run(compiled, initFailedOnce)
      _             <- started.get.assertEquals(true).eventually
    } yield ()
  }

  private def assertCrash(metadata: ProjectionMetadata, strategy: ExecutionStrategy)(using Location) = {
    val expectedException = new IllegalStateException("The stream crashed unexpectedly.")
    for {
      started       <- Ref.of[IO, Boolean](false)
      alreadyFailed <- Ref.of[IO, Boolean](false)
      failingOnce    = Stream.eval(alreadyFailed.get).flatMap {
                         case true  => Stream.never[IO]
                         case false => Stream.eval(alreadyFailed.set(true)) >> Stream.raiseError[IO](expectedException)
                       }
      projection     =
        CompiledProjection.fromStream(metadata, strategy, evalStream(started.set(true)).map(_ >> failingOnce))
      _             <- sv.run(projection)
      _             <- started.get.assertEquals(true).eventually
    } yield ()
  }

  private def assertDestroy(
      metadata: ProjectionMetadata,
      strategy: ExecutionStrategy,
      onDestroy: IO[Unit]
  )(using Location) = {
    // The CompiledProjection passed to `destroy` is only consulted for its metadata and execution strategy;
    // a no-op stream is sufficient.
    val compiled = CompiledProjection.noop(metadata, strategy)
    for {
      _ <- sv.destroy(compiled, onDestroy).map(_.map(_.isTerminal)).assertEquals(Some(true))
      _ <- sv.describe(metadata.name).assertEquals(None).eventually
      _ <- projections.progress(metadata.name).assertEquals(None)
    } yield ()
  }

  private def assertDescribe(
      metadata: ProjectionMetadata,
      executionStrategy: ExecutionStrategy,
      restarts: Int,
      status: ExecutionStatus,
      progress: ProjectionProgress
  )(using loc: Location) =
    sv.describe(metadata.name)
      .assertEquals(
        Some(SupervisedDescription(metadata, executionStrategy, restarts, status, progress))
      )
      .eventually

  private def assertAbsent(metadata: ProjectionMetadata)(using loc: Location) =
    sv.describe(metadata.name)
      .assertEquals(None)

  private def assertWatchRestarts(offset: Offset, processed: Long, discarded: Long)(using Location) = {
    val progress = ProjectionProgress(offset, Instant.EPOCH, processed, discarded, 0)
    assertDescribe(WatchRestarts.projectionMetadata, EveryNode, 0, Running, progress)
  }

  private def assertNoSavedProgress(metadata: ProjectionMetadata)(using loc: Location) =
    projections.progress(metadata.name).assertEquals(None)

  private def assertSavedProgress(metadata: ProjectionMetadata, progress: ProjectionProgress)(using loc: Location) =
    projections.progress(metadata.name).assertEquals(Some(progress))

  /** Returns the [[ProjectionTermination.Completed]] row for `name`, if any. */
  private def findCompleted(name: String): IO[Option[ProjectionTermination.Completed]] =
    terminalLog.stream.compile.toList.map { events =>
      events.collectFirst { case c: ProjectionTermination.Completed if c.metadata.name == name => c }
    }

  /** Returns the [[ProjectionTermination.Failed]] row for `name`, if any. */
  private def findFailed(name: String): IO[Option[ProjectionTermination.Failed]] =
    terminalLog.stream.compile.toList.map { events =>
      events.collectFirst { case f: ProjectionTermination.Failed if f.metadata.name == name => f }
    }

  test("Watching restart projection restarts should be running") {
    assertWatchRestarts(Offset.Start, 0, 0)
  }

  test("Ignore a projection when it is meant to run on another node") {
    for {
      flag      <- Ref.of[IO, Boolean](false)
      projection =
        CompiledProjection.fromStream(
          ignoredByNode1,
          TransientSingleNode,
          evalStream(flag.set(true))
        )
      _         <- sv.run(projection).assertEquals(None)
      _         <- IO.sleep(100.millis)
      // The projection should still be ignored and should not have made any progress
      _         <- assertAbsent(ignoredByNode1)
      // No progress has been saved in database either
      _         <- assertNoSavedProgress(ignoredByNode1)
      // This means the stream has never been started
      _         <- flag.get.assertEquals(false)
    } yield ()
  }

  test("Do nothing when attempting to restart a projection when it is meant to run on another node") {
    for {
      _ <- projections.scheduleRestart(ignoredByNode1, Offset.start)
      _ <- assertWatchRestarts(Offset.at(1L), 1, 1)
      _ <- assertAbsent(ignoredByNode1)
      // The restart has not been acknowledged and can be read by another node
      _ <- projections.restarts(Offset.start).assertSize(1)
    } yield ()
  }

  test("Destroy an ignored projection") {
    val compiled = CompiledProjection.noop(ignoredByNode1, TransientSingleNode)
    sv.destroy(compiled).assertEquals(None)
  }

  test("Do nothing when attempting to restart a projection when it is unknown") {
    for {
      _ <- projections.scheduleRestart(ProjectionMetadata("test", "xxx"), Offset.start)
      _ <- assertWatchRestarts(Offset.at(2L), 2, 2)
      _ <- assertAbsent(ignoredByNode1)
      // The restart has not been acknowledged and can be read by another node
      _ <- projections.restarts(Offset.at(1L)).assertSize(1)
    } yield ()
  }

  test("Destroy an unknown projection") {
    val unknown  = ProjectionMetadata("test", "xxx", None, None)
    val compiled = CompiledProjection.noop(unknown, TransientSingleNode)
    sv.destroy(compiled).assertEquals(None)
  }

  test("Run a projection when it is meant to run on every node") {
    for {
      _ <- startProjection(random, EveryNode)
      // The projection should have completed and been evicted from supervision
      _ <- assertAbsent(random).eventually
      // As it runs on every node, it is implicitly transient so no progress has been saved to database
      _ <- assertNoSavedProgress(random)
    } yield ()
  }

  test("Run a transient projection when it is meant to run on this node") {
    for {
      _ <- startProjection(runnableByNode1, TransientSingleNode)
      // The projection should have completed and been evicted from supervision
      _ <- assertAbsent(runnableByNode1).eventually
      // As it is transient, no progress has been saved to database
      _ <- assertNoSavedProgress(runnableByNode1)
      // A completion termination has been recorded
      _ <- findCompleted(runnableByNode1.name).map(_.map(_.occurrences)).assertEquals(Some(1L))
    } yield ()
  }

  test("Destroy a registered transient projection") {
    assertDestroy(runnableByNode1, TransientSingleNode, IO.unit)
  }

  test("Run a persistent projection when it is meant to run on this node") {
    for {
      _ <- startProjection(runnableByNode1, PersistentSingleNode)
      // The projection should have completed and been evicted from supervision
      _ <- assertAbsent(runnableByNode1).eventually
      // As it is persistent, progress has been saved to database and is preserved across eviction
      _ <- assertSavedProgress(runnableByNode1, expectedProgress)
      // The completion termination row is upserted (same key as the transient run above)
      _ <- findCompleted(runnableByNode1.name).map(_.map(_.occurrences)).assertEquals(Some(2L))
    } yield ()
  }

  test("Re-running a persistent projection with no new data does not record a termination") {
    // Persisted offset is 20 from the prior run. Re-running with the same range produces zero new elements
    // (every elem is filtered by the persist pipe), so `didWork = false` and the listener does not write.
    for {
      _ <- startProjection(runnableByNode1, PersistentSingleNode)
      _ <- assertAbsent(runnableByNode1).eventually
      _ <- assertSavedProgress(runnableByNode1, expectedProgress)
      // occurrences remains at 2, not 3
      _ <- findCompleted(runnableByNode1.name).map(_.map(_.occurrences)).assertEquals(Some(2L))
    } yield ()
  }

  test("Resume an evicted projection on the responsible node instead of dropping the restart") {
    // After the prior persistent-projection test completed, the projection has been evicted from supervision.
    // On the responsible node the restart resets the offset and broadcasts an activation (so coordinators can resume
    // it) and the record is acknowledged — it is processed (not dropped, hence discarded stays at 2).
    val expectedProgress = ProjectionProgress(Offset.start, Instant.EPOCH, 0, 0, 0)
    for {
      _ <- projections.scheduleRestart(runnableByNode1, Offset.start)
      _ <- assertWatchRestarts(Offset.at(3L), 3, 2)
      _ <- assertAbsent(runnableByNode1)
      _ <- assertSavedProgress(runnableByNode1, expectedProgress)
      _ <- projections.restarts(Offset.at(2L)).assertEmpty
    } yield ()
  }

  test("Destroy a registered persistent projection") {
    assertDestroy(runnableByNode1, PersistentSingleNode, IO.unit)
  }

  test("Should restart a failing projection") {
    val expectedErrorClass = classOf[IllegalStateException].getName
    for {
      _ <- assertCrash(runnableByNode1, TransientSingleNode)
      _ <- sv.describe(runnableByNode1.name).map(_.map(_.restarts)).assertEquals(Some(1)).eventually
      // The failure has been recorded with its error class
      _ <- findFailed(runnableByNode1.name).map(_.map(_.errorClass)).assertEquals(Some(expectedErrorClass)).eventually
    } yield ()
  }

  test("Destroy the still-running failed projection") {
    // The restart test above leaves the projection running (its stream never completes), so it must be destroyed
    // before the name can be reused: `run` is idempotent and would otherwise ignore a re-run while the old one is up.
    assertDestroy(runnableByNode1, TransientSingleNode, IO.unit)
  }

  test("Should restart a projection when init fails") {
    // After the init retry succeeds, the stream completes and the projection is evicted from supervision
    for {
      _ <- assertInitCrash(runnableByNode1, TransientSingleNode)
      _ <- assertAbsent(runnableByNode1).eventually
      // The completion termination row is upserted again (occurrences = 3)
      _ <- findCompleted(runnableByNode1.name).map(_.map(_.occurrences)).assertEquals(Some(3L))
    } yield ()
  }

  test("Obtain the correct running projections") {
    val watchRestartProgress = ProjectionProgress(Offset.at(3L), Instant.EPOCH, 3, 2, 0)
    for {
      _ <- startProjection(runnableByNode1, PersistentSingleNode)
      // The persistent projection completes and is evicted from supervision, leaving only the long-running
      // EveryNode system projection.
      _ <- sv.getRunningProjections
             .assertEquals(
               List(
                 SupervisedDescription(
                   WatchRestarts.projectionMetadata,
                   EveryNode,
                   restarts = 0,
                   Running,
                   progress = watchRestartProgress
                 )
               )
             )
             .eventually
    } yield ()
  }

  test("Run and properly destroy a projection with an unstable destroy method") {
    val projection = ProjectionMetadata("test", "unstable-global-projection", None, None)
    for {
      _               <- startProjection(projection, EveryNode)
      // Destroy the projection with a destroy method that fails and eventually succeeds
      unstableDestroy <- UnstableDestroy()
      _               <- assertDestroy(projection, EveryNode, unstableDestroy.attempt)
      _               <- unstableDestroy.isCompleted.assertEquals(true, "The destroy method should have completed")
    } yield ()
  }

  test("Run and properly destroy a projection with an failing destroy method") {
    val projection = ProjectionMetadata("test", "unstable-global-projection", None, None)
    val alwaysFail = IO.raiseError(new IllegalStateException("Fail !"))
    for {
      _ <- startProjection(projection, EveryNode)
      _ <- assertDestroy(projection, EveryNode, alwaysFail)
    } yield ()
  }
}

object SupervisorSuite {

  /**
    * Creates a destroy method which eventually succeeds after a couple of failures
    */
  final class UnstableDestroy(count: Ref[IO, Int], completed: Ref[IO, Boolean]) {
    def attempt: IO[Unit] = count
      .updateAndGet(_ + 1)
      .flatMap { i => IO.raiseWhen(i < 2)(new IllegalStateException(s"'$i' is lower than 2.")) }
      .flatTap { _ => completed.set(true) }

    def isCompleted: IO[Boolean] = completed.get
  }

  object UnstableDestroy {
    def apply(): IO[UnstableDestroy] =
      for {
        count     <- Ref.of[IO, Int](0)
        completed <- Ref.of[IO, Boolean](false)
      } yield new UnstableDestroy(count, completed)
  }

}
