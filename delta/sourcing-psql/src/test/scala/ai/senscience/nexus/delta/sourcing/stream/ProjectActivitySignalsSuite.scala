package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectLastUpdate
import ai.senscience.nexus.testkit.clock.MutableClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*
import fs2.Stream
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.DurationInt

class ProjectActivitySignalsSuite extends NexusSuite with MutableClock.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(mutableClockFixture)
  private lazy val mutableClock: MutableClock    = mutableClockFixture()

  private def assertActivitySignal(
      lastUpdateReads: ProjectActivitySignals,
      project: ProjectRef,
      expected: Option[Boolean]
  )(implicit loc: Location) =
    lastUpdateReads.apply(project).flatMap(_.traverse(_.get)).assertEquals(expected)

  test("Signals should be updated when the stream is processed") {
    val now              = Instant.now()
    val inactiveInterval = 5.seconds

    val project1 = ProjectRef.unsafe("org", "project1")
    val project2 = ProjectRef.unsafe("org", "project2")
    val project3 = ProjectRef.unsafe("org", "project3")
    val project4 = ProjectRef.unsafe("org", "project4")

    def stream =
      Stream(
        ProjectLastUpdate(project1, now.minusSeconds(15L), Offset.at(35L)),
        ProjectLastUpdate(project2, now.minusSeconds(6L), Offset.at(42L)),
        ProjectLastUpdate(project3, now, Offset.at(95L)),
        ProjectLastUpdate(project1, now, Offset.at(100L))
      )

    for {
      signals        <- ProjectSignals[ProjectLastUpdate]
      _              <- mutableClock.set(now)
      lastUpdateReads = ProjectActivitySignals(signals)
      signalPipe      = ProjectActivitySignals.signalPipe(signals, mutableClock, inactiveInterval)
      _              <- stream.through(signalPipe).compile.drain
      _              <- assertActivitySignal(lastUpdateReads, project1, Some(true))
      _              <- assertActivitySignal(lastUpdateReads, project2, Some(false))
      _              <- assertActivitySignal(lastUpdateReads, project3, Some(true))
      _              <- assertActivitySignal(lastUpdateReads, project4, None)
    } yield ()
  }

}
