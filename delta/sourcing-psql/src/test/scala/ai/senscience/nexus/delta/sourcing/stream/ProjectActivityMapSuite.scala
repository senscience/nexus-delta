package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.clock.MutableClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.DurationInt

class ProjectActivityMapSuite extends NexusSuite with MutableClock.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(mutableClockFixture)

  private lazy val mutableClock: MutableClock = mutableClockFixture()

  private val inactiveInterval = 5.seconds

  private def assertActive(activity: ProjectActivityMap, project: ProjectRef, expected: Boolean)(using Location) =
    activity.isActive(project).assertEquals(expected)

  test("newValues records every reported project as active and returns only the fresh ones as transitions") {
    val now = Instant.now()

    val fresh = ProjectRef.unsafe("org", "fresh")
    val stale = ProjectRef.unsafe("org", "stale")

    for {
      _           <- mutableClock.set(now)
      activity    <- ProjectActivityMap(mutableClock, inactiveInterval)
      // Frontier starts at `now`; threshold = now - 5. `fresh` transitions, `stale` does not.
      transitions <- activity.newValues(List(fresh -> now, stale -> now.minusSeconds(10L)))
      _           <- IO(assertEquals(transitions, Set(fresh)))
      // Both are recorded as active regardless of freshness; eviction is deferred to `refresh`.
      _           <- assertActive(activity, fresh, true)
      _           <- assertActive(activity, stale, true)
      // `refresh` (wall clock) evicts the stale one.
      _           <- activity.refresh
      _           <- assertActive(activity, fresh, true)
      _           <- assertActive(activity, stale, false)
    } yield ()
  }

  test("newValues judges transitions against the activity frontier, not the wall clock") {
    val now = Instant.now()

    val project = ProjectRef.unsafe("org", "proj")

    for {
      _           <- mutableClock.set(now)
      activity    <- ProjectActivityMap(mutableClock, inactiveInterval)
      // The wall clock jumps far ahead (as if activity were reported late), yet an update at the frontier still
      // transitions to active: activation ignores the wall clock.
      _           <- mutableClock.set(now.plusSeconds(3600L))
      transitions <- activity.newValues(List(project -> now))
      _           <- IO(assertEquals(transitions, Set(project)))
    } yield ()
  }

  test("an already active project does not transition again") {
    val now = Instant.now()

    val project = ProjectRef.unsafe("org", "proj")

    for {
      _        <- mutableClock.set(now)
      activity <- ProjectActivityMap(mutableClock, inactiveInterval)
      first    <- activity.newValues(List(project -> now))
      _        <- IO(assertEquals(first, Set(project)))
      second   <- activity.newValues(List(project -> now))
      _        <- IO(assertEquals(second, Set.empty[ProjectRef]))
    } yield ()
  }

  test("refresh evicts idle projects, measured against the wall clock") {
    val now = Instant.now()

    val project1 = ProjectRef.unsafe("org", "proj1")
    val project2 = ProjectRef.unsafe("org", "proj2")

    for {
      _        <- mutableClock.set(now)
      activity <- ProjectActivityMap(mutableClock, inactiveInterval)
      _        <- activity.newValues(List(project1 -> now, project2 -> now))
      _        <- assertActive(activity, project1, true)
      _        <- assertActive(activity, project2, true)
      // No new activity moves the frontier, but the wall clock advances past the inactivity window: refresh evicts.
      _        <- mutableClock.set(now.plusSeconds(10L))
      _        <- activity.refresh
      _        <- assertActive(activity, project1, false)
      _        <- assertActive(activity, project2, false)
    } yield ()
  }

  test("a project transitions again after being evicted") {
    val now = Instant.now()

    val project = ProjectRef.unsafe("org", "proj")

    for {
      _        <- mutableClock.set(now)
      activity <- ProjectActivityMap(mutableClock, inactiveInterval)
      first    <- activity.newValues(List(project -> now))
      _        <- IO(assertEquals(first, Set(project)))
      _        <- mutableClock.set(now.plusSeconds(10L))
      _        <- activity.refresh
      _        <- assertActive(activity, project, false)
      // Reported fresh again → transition (it was inactive).
      again    <- activity.newValues(List(project -> now.plusSeconds(10L)))
      _        <- IO(assertEquals(again, Set(project)))
    } yield ()
  }

  test("activeProjects returns the projects retained after refresh") {
    val now = Instant.now()

    val active1  = ProjectRef.unsafe("org", "active1")
    val active2  = ProjectRef.unsafe("org", "active2")
    val inactive = ProjectRef.unsafe("org", "inactive")

    for {
      _        <- mutableClock.set(now)
      activity <- ProjectActivityMap(mutableClock, inactiveInterval)
      _        <- activity.newValues(List(active1 -> now, active2 -> now, inactive -> now.minusSeconds(10L)))
      _        <- activity.refresh
      _        <- activity.activeProjects.map(_.toSet).assertEquals(Set(active1, active2))
    } yield ()
  }
}
