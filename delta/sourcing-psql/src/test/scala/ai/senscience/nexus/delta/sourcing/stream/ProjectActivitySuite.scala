package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
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
      projectActivity = ProjectActivity(activityMap)
      activityPipe    = ProjectActivity.activityPipe(activityMap)
      _              <- stream.through(activityPipe).compile.drain
      _              <- projectActivity(project1).assertEquals(true)
      _              <- projectActivity(project2).assertEquals(false)
      _              <- projectActivity(project3).assertEquals(true)
      _              <- projectActivity(project4).assertEquals(false)
    } yield ()
  }

}
