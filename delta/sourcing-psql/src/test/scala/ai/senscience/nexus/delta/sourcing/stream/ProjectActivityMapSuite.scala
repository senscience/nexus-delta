package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.clock.MutableClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.DurationInt

class ProjectActivityMapSuite extends NexusSuite with MutableClock.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(mutableClockFixture)

  private lazy val mutableClock: MutableClock = mutableClockFixture()

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
      activity <- ProjectActivityMap(mutableClock, inactiveInterval)
      _        <- mutableClock.set(now.minusSeconds(5L))
      _        <- activity.newValues(init)
      _        <- activity.contains(project1).assertEquals(false)
      _        <- activity.contains(project2).assertEquals(true)
      _        <- activity.contains(project3).assertEquals(true)
      _        <- activity.contains(project4).assertEquals(false)
      _        <- mutableClock.set(now)
      updates   = List(project4 -> now.minusSeconds(2L), project1 -> now)
      _        <- activity.newValues(updates)
      _        <- activity.evictInactiveProjects
      _        <- activity.contains(project1).assertEquals(true)
      _        <- activity.contains(project2).assertEquals(false)
      _        <- activity.contains(project3).assertEquals(true)
      _        <- activity.contains(project4).assertEquals(true)
    } yield ()
  }

}
