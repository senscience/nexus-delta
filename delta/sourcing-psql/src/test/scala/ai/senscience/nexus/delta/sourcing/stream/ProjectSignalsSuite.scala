package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.clock.MutableClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*
import munit.{AnyFixture, Location}

import java.time.Instant

import scala.concurrent.duration.DurationInt

class ProjectSignalsSuite extends NexusSuite with MutableClock.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(mutableClockFixture)

  private lazy val mutableClock: MutableClock = mutableClockFixture()

  private def assertSignal(signals: ProjectSignals, project: ProjectRef, expected: Option[Boolean])(using
      loc: Location
  ) =
    signals.getSignal(project).flatMap(_.traverse(_.get)).assertEquals(expected)

  test("Should init and update the signals accordingly") {
    val now              = Instant.now()
    val inactiveInterval = 5.seconds

    val project1 = ProjectRef.unsafe("org", "proj1")
    val project2 = ProjectRef.unsafe("org", "proj2")
    val project3 = ProjectRef.unsafe("org", "proj3")
    val project4 = ProjectRef.unsafe("org", "proj4")

    val init = List(
      project1 -> now.minusSeconds(11L),
      project2 -> now.minusSeconds(6L),
      project3 -> now.minusSeconds(5L)
    )

    for {
      signals <- ProjectSignals(mutableClock, inactiveInterval)
      _       <- mutableClock.set(now.minusSeconds(5L))
      _       <- signals.newValues(init)
      _       <- assertSignal(signals, project1, Some(false))
      _       <- assertSignal(signals, project2, Some(true))
      _       <- assertSignal(signals, project3, Some(true))
      _       <- assertSignal(signals, project4, None)
      _       <- signals.getActiveProjects.assertEquals(Set(project2, project3))
      _       <- mutableClock.set(now)
      updates  = List(project4 -> now.minusSeconds(2L), project1 -> now)
      _       <- signals.newValues(updates)
      _       <- signals.evictInactiveProjects
      _       <- assertSignal(signals, project1, Some(true))
      _       <- assertSignal(signals, project2, Some(false))
      _       <- assertSignal(signals, project3, Some(true))
      _       <- assertSignal(signals, project4, Some(true))
      _       <- signals.getActiveProjects.assertEquals(Set(project1, project3, project4))
    } yield ()
  }

}
