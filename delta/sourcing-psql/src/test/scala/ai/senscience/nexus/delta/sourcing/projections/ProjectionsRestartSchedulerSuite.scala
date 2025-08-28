package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectionRestart
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.delta.sourcing.stream.{ProjectionMetadata, ProjectionProgress}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import munit.AnyFixture
import fs2.Stream

import java.time.Instant

final class ProjectionsRestartSchedulerSuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas              = doobie()
  private lazy val projections      = Projections(xas, None, QueryConfig(10, RefreshStrategy.Stop), clock)
  private lazy val restartScheduler = ProjectionsRestartScheduler(projections)

  test("Restart the given projections if their progress is greater than the provided offset") {
    val fromOffset    = Offset.at(42L)
    val greaterOffset = ProjectionMetadata("test", "greater-offset", None, None)
    val smallerOffset = ProjectionMetadata("test", "smaller-offset", None, None)
    val noOffset      = ProjectionMetadata("test", "no-offset", None, None)

    implicit val subject: Subject = Anonymous

    val projectionStream = Stream
      .iterable(List(greaterOffset, smallerOffset, noOffset))
      .map(_.name)
      .covary[IO]

    def progress(offset: Offset) = ProjectionProgress(offset, Instant.now(), offset.value, 0L, 0L)

    for {
      _ <- projections.save(greaterOffset, progress(fromOffset))
      _ <- projections.save(smallerOffset, progress(Offset.at(41L)))
      _ <- restartScheduler.run(projectionStream, fromOffset)
      _ <- projections
             .restarts(Offset.start)
             .map(_._2)
             .assert(
               ProjectionRestart(greaterOffset.name, fromOffset, Instant.EPOCH, subject)
             )
    } yield ()
  }

}
