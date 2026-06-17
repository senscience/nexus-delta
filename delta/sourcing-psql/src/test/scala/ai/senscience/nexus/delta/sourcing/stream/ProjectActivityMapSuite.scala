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

  private def assertActive(activity: ProjectActivityMap, project: ProjectRef, expected: Boolean)(using Location) =
    activity.isActive(project).assertEquals(expected)

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
      _        <- assertActive(activity, project1, false)
      _        <- assertActive(activity, project2, true)
      _        <- assertActive(activity, project3, true)
      _        <- assertActive(activity, project4, false)
      _        <- mutableClock.set(now)
      updates   = List(project4 -> now.minusSeconds(2L), project1 -> now)
      _        <- activity.newValues(updates)
      _        <- activity.refresh
      _        <- assertActive(activity, project1, true)
      _        <- assertActive(activity, project2, false)
      _        <- assertActive(activity, project3, true)
      _        <- assertActive(activity, project4, true)
    } yield ()
  }

  test("newValues returns the set of projects that transitioned to active") {
    val now              = Instant.now()
    val inactiveInterval = 5.seconds

    val newActive   = ProjectRef.unsafe("org", "new-active")
    val newInactive = ProjectRef.unsafe("org", "new-inactive")
    val reactivated = ProjectRef.unsafe("org", "reactivated")
    val staysActive = ProjectRef.unsafe("org", "stays-active")
    val goesIdle    = ProjectRef.unsafe("org", "goes-idle")

    for {
      activity    <- ProjectActivityMap(mutableClock, inactiveInterval)
      _           <- mutableClock.set(now)
      // Batch 1: newActive (insert+active → transition), newInactive (insert+inactive → none),
      //          reactivated (insert+inactive → none), staysActive (insert+active → transition),
      //          goesIdle (insert+active → transition)
      firstBatch  <- activity.newValues(
                       List(
                         newActive   -> now,
                         newInactive -> now.minusSeconds(10L),
                         reactivated -> now.minusSeconds(10L),
                         staysActive -> now,
                         goesIdle    -> now
                       )
                     )
      _           <- IO(assertEquals(firstBatch, Set(newActive, staysActive, goesIdle)))
      // Batch 2: reactivated goes inactive → active (transition), staysActive stays active (no transition),
      //          goesIdle becomes inactive (no transition)
      _           <- mutableClock.set(now.plusSeconds(10L))
      secondBatch <- activity.newValues(
                       List(
                         reactivated -> now.plusSeconds(10L),
                         staysActive -> now.plusSeconds(10L),
                         goesIdle    -> now // now older than threshold relative to new clock
                       )
                     )
      _           <- IO(assertEquals(secondBatch, Set(reactivated)))
    } yield ()
  }

  test("refresh returns the set of projects that transitioned to active") {
    val now              = Instant.now()
    val inactiveInterval = 5.seconds

    val project = ProjectRef.unsafe("org", "reactivated")

    for {
      activity <- ProjectActivityMap(mutableClock, inactiveInterval)
      _        <- mutableClock.set(now)
      // Insert as inactive — no transition
      first    <- activity.newValues(List(project -> now.minusSeconds(10L)))
      _        <- IO(assertEquals(first, Set.empty[ProjectRef]))
      // Promote to active — one transition
      second   <- activity.newValues(List(project -> now))
      _        <- IO(assertEquals(second, Set(project)))
      // Advance the clock so the existing fresh instant becomes inactive; refresh flips to inactive (no transition)
      _        <- mutableClock.set(now.plusSeconds(10L))
      _        <- activity.refresh
      // Bump back to fresh — transition again
      fourth   <- activity.newValues(List(project -> now.plusSeconds(10L)))
      _        <- IO(assertEquals(fourth, Set(project)))
    } yield ()
  }

  test("activeProjects returns the snapshot of projects currently flagged active") {
    val now              = Instant.now()
    val inactiveInterval = 5.seconds

    val active1  = ProjectRef.unsafe("org", "active1")
    val active2  = ProjectRef.unsafe("org", "active2")
    val inactive = ProjectRef.unsafe("org", "inactive")

    for {
      activity <- ProjectActivityMap(mutableClock, inactiveInterval)
      _        <- mutableClock.set(now)
      _        <- activity.newValues(
                    List(active1 -> now, active2 -> now, inactive -> now.minusSeconds(10L))
                  )
      _        <- activity.activeProjects.map(_.toSet).assertEquals(Set(active1, active2))
    } yield ()
  }
}
