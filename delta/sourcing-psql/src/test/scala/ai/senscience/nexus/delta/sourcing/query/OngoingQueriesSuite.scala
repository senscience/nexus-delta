package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming.QueryStatus
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming.QueryStatus.Ongoing
import ai.senscience.nexus.delta.sourcing.query.OngoingQueries.Execute
import ai.senscience.nexus.delta.sourcing.query.OngoingQueriesSuite.{QueryAttempt, RunSet, WaitSet}
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import munit.Location

import java.util.UUID
import scala.concurrent.duration.DurationInt

class OngoingQueriesSuite extends NexusSuite {

  private given PatienceConfig = PatienceConfig(1.second, 10.millis)

  private def execQuery(io: IO[Unit], continue: Boolean)(using running: RunSet, waiting: WaitSet) = {
    val queryStatus = Ongoing(UUID.randomUUID(), Offset.start)

    val next =
      if continue then Some(("Next", queryStatus))
      else Some(("Next", queryStatus.suspended(RefreshOrStop.RefreshOutcome.Passivated)))

    def ifRunnable(q: QueryStatus) = waiting.delete(q) >> running.add(q) >> io.as(next)

    def ifWaiting(q: QueryStatus) = waiting.add(q).as(None)

    QueryAttempt(queryStatus, ifRunnable, ifWaiting)
  }

  private def tryRun(query: QueryAttempt)(using ongoing: OngoingQueries) =
    ongoing.tryRun(query.status)(query.onRun, query.onWait)

  private def isOngoing(query: QueryAttempt)(using ongoing: OngoingQueries)(using Location) =
    ongoing.contains(query.status.uuid).assertEquals(true).eventually

  private def isNotOngoing(query: QueryAttempt)(using ongoing: OngoingQueries)(using Location) =
    ongoing.contains(query.status.uuid).assertEquals(false).eventually

  private def hasRun(query: QueryAttempt)(using running: RunSet)(using Location) =
    running.contains(query.status).assertEquals(true).eventually

  private def hasNotRun(query: QueryAttempt)(using running: RunSet)(using Location) =
    running.contains(query.status).assertEquals(false)

  private def hasWaited(query: QueryAttempt)(using waiting: WaitSet)(using Location) =
    waiting.contains(query.status).assertEquals(true).eventually

  private def hasNotWaited(query: QueryAttempt)(using waiting: WaitSet)(using Location) =
    waiting.contains(query.status).assertEquals(false)

  test("Only one query can be run at a time and the second can't start if the first one is still marked as ongoing") {
    (RunSet(), WaitSet(), OngoingQueries(1)).flatMapN { case (running, waiting, ongoing) =>
      given OngoingQueries = ongoing

      given RunSet = running

      given WaitSet = waiting

      val query1 = execQuery(IO.sleep(100.millis), true)
      val query2 = execQuery(IO.sleep(100.millis), false)
      for {
        // First query should run and the second query should be pushed back
        _ <- tryRun(query1).start
        _ <- (IO.sleep(50.millis) >> tryRun(query2)).start
        _ <- isOngoing(query1)
        _ <- hasRun(query1)
        _ <- hasNotWaited(query1)
        _ <- isNotOngoing(query2)
        _ <- hasNotRun(query2)
        _ <- hasWaited(query2)
        // The first query should eventually complete but is still ongoing
        _ <- isOngoing(query1)
        // Second query still can't be run
        _ <- tryRun(query2).start
        _ <- isNotOngoing(query2)
        _ <- hasNotRun(query2)
      } yield ()
    }
  }

  test("Only one query can be run at a time and the second can take over when the first completes") {
    (RunSet(), WaitSet(), OngoingQueries(1)).flatMapN { case (running, waiting, ongoing) =>
      given OngoingQueries = ongoing
      given RunSet         = running
      given WaitSet        = waiting

      val query1 = execQuery(IO.sleep(100.millis), false)
      val query2 = execQuery(IO.sleep(100.millis), false)
      for {
        // First query should run and the second query should be pushed back
        _ <- tryRun(query1).start
        _ <- (IO.sleep(50.millis) >> tryRun(query2)).start
        _ <- isOngoing(query1)
        _ <- hasRun(query1)
        _ <- hasNotWaited(query1)
        _ <- isNotOngoing(query2)
        _ <- hasNotRun(query2)
        _ <- hasWaited(query2)
        // The first query should eventually complete
        _ <- isNotOngoing(query1)
        // Second query can be run now
        _ <- tryRun(query2).start
        _ <- isOngoing(query2)
        _ <- hasRun(query2)
        _ <- hasNotWaited(query2)
        // The second query should eventually complete
        _ <- isNotOngoing(query2)
      } yield ()
    }
  }

  test("Failing query should also free the permit") {
    (RunSet(), WaitSet(), OngoingQueries(1)).flatMapN { case (running, waiting, ongoing) =>
      given OngoingQueries = ongoing
      given RunSet         = running
      given WaitSet        = waiting

      val query = execQuery(IO.sleep(100.millis) >> IO.raiseError(new IllegalStateException()), false)
      for {
        _ <- tryRun(query).start
        _ <- isOngoing(query)
        _ <- hasRun(query)
        _ <- isNotOngoing(query)
      } yield ()
    }
  }

  test("Cancelled query should also free the permit") {
    (RunSet(), WaitSet(), OngoingQueries(1)).flatMapN { case (running, waiting, ongoingQueries) =>
      given OngoingQueries = ongoingQueries
      given RunSet         = running
      given WaitSet        = waiting

      val query = execQuery(IO.sleep(100.millis) >> IO.canceled, false)
      for {
        _ <- tryRun(query).start
        _ <- isOngoing(query)
        _ <- hasRun(query)
        _ <- isNotOngoing(query)
      } yield ()
    }
  }

}

object OngoingQueriesSuite {
  opaque type RunSet = Ref[IO, Set[QueryStatus]]

  object RunSet {
    def apply(): IO[RunSet] = Ref.of[IO, Set[QueryStatus]](Set.empty)

    extension (r: RunSet) {
      def contains(status: QueryStatus): IO[Boolean] = r.get.map(_.contains(status))
      def add(status: QueryStatus): IO[Unit]         = r.update(_ + status)
    }
  }

  opaque type WaitSet = Ref[IO, Set[QueryStatus]]

  object WaitSet {
    def apply(): IO[WaitSet] = Ref.of[IO, Set[QueryStatus]](Set.empty)

    extension (w: WaitSet) {
      def contains(status: QueryStatus): IO[Boolean] = w.get.map(_.contains(status))
      def add(status: QueryStatus): IO[Unit]         = w.update(_ + status)
      def delete(status: QueryStatus): IO[Unit]      = w.update(_ - status)
    }
  }

  final case class QueryAttempt(status: QueryStatus, onRun: Execute[String], onWait: Execute[String])

}
