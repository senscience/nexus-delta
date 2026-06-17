package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectLastUpdateStream
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectLastUpdate
import ai.senscience.nexus.testkit.clock.MutableClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import fs2.Stream
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.DurationInt

class ProjectActivitySuite extends NexusSuite with MutableClock.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(mutableClockFixture)
  private lazy val mutableClock: MutableClock    = mutableClockFixture()

  private def assertActive(activity: ProjectActivity, project: ProjectRef, expected: Boolean)(using Location) =
    activity.isActive(project).assertEquals(expected)

  test("Project activity should be updated when the stream is processed") {
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
      activityMap    <- ProjectActivityMap(mutableClock, inactiveInterval)
      _              <- mutableClock.set(now)
      projectActivity = ProjectActivity(activityMap, ProjectionActivations.noop)
      activityPipe    = ProjectActivity.activityPipe(activityMap, ProjectionActivations.noop)
      _              <- stream.through(activityPipe).compile.drain
      _              <- assertActive(projectActivity, project1, true)
      _              <- assertActive(projectActivity, project2, false)
      _              <- assertActive(projectActivity, project3, true)
      _              <- assertActive(projectActivity, project4, false)
    } yield ()
  }

  test("Active projects should be seeded from the store at startup") {
    val now              = Instant.now()
    val inactiveInterval = 5.seconds

    val project1 = ProjectRef.unsafe("org", "project1")
    val project2 = ProjectRef.unsafe("org", "project2")

    val stream = new ProjectLastUpdateStream {
      override def apply(offset: Offset): Stream[IO, ProjectLastUpdate]       = Stream.empty
      // Emulates the store's `TimeRange.After(threshold)` query: it only yields projects updated within the
      // inactivity window, so their last-update instants are more recent than `now - inactiveInterval`.
      override def apply(timeRange: TimeRange): Stream[IO, ProjectLastUpdate] =
        Stream(
          ProjectLastUpdate(project1, now.minusSeconds(4L), Offset.at(35L)),
          ProjectLastUpdate(project2, now.minusSeconds(2L), Offset.at(42L))
        )
    }

    for {
      activityMap <- ProjectActivityMap(mutableClock, inactiveInterval)
      _           <- mutableClock.set(now)
      _           <- ProjectActivity.seedActiveProjects(stream, activityMap, mutableClock, inactiveInterval)
      _           <- activityMap.activeProjects.map(_.toSet).assertEquals(Set(project1, project2))
    } yield ()
  }

}
