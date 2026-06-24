package ai.senscience.nexus.delta.plugins.projectdeletion

import ai.senscience.nexus.delta.projectdeletion.ProjectDeletionRunner
import ai.senscience.nexus.delta.projectdeletion.ProjectDeletionRunner.ProjectDeletionCandidate
import ai.senscience.nexus.delta.projectdeletion.model.ProjectDeletionConfig
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}
import fs2.Stream

import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.matching.Regex

class ProjectDeletionRunnerSuite extends NexusSuite {

  private def candidate(
      project: String,
      deprecated: Boolean,
      rev: Int = 1,
      updatedAt: Instant = Instant.EPOCH,
      markedForDeletion: Boolean = false
  ): ProjectDeletionCandidate =
    ProjectDeletionCandidate(ProjectRef.unsafe("org", project), rev, deprecated, markedForDeletion, updatedAt)

  private def config(
      deleteDeprecatedProjects: Boolean,
      idleInterval: FiniteDuration = 3650.days,
      included: List[Regex] = List(".*".r),
      excluded: List[Regex] = Nil
  ): ProjectDeletionConfig =
    ProjectDeletionConfig(idleInterval, idleCheckPeriod = 1.day, deleteDeprecatedProjects, included, excluded)

  /**
    * Builds a runner from plain functions (no need to stub the whole `Projects`/`ProjectsStatistics`) and exposes a way
    * to read back the projects that were deleted as `(ref, rev)`.
    */
  private def runnerWith(
      cfg: ProjectDeletionConfig,
      candidates: List[ProjectDeletionCandidate],
      lastEventTime: ProjectRef => IO[Option[Instant]] = _ => IO.pure(Some(Instant.EPOCH)),
      failDeletionFor: Set[ProjectRef] = Set.empty
  ): IO[(ProjectDeletionRunner, IO[List[(ProjectRef, Int)]])] =
    Ref.of[IO, List[(ProjectRef, Int)]](Nil).map { deleted =>
      val delete: (ProjectRef, Int) => IO[Unit] = (ref, rev) =>
        IO.raiseWhen(failDeletionFor.contains(ref))(new RuntimeException("boom")) >> deleted.update(_ :+ (ref -> rev))
      val runner                                =
        new ProjectDeletionRunner(cfg, Stream.emits(candidates), lastEventTime, delete, clock)
      (runner, deleted.get)
    }

  test("delete only the projects selected by the criteria") {
    val deprecated = candidate("old", deprecated = true, rev = 2)
    val active     = candidate("live", deprecated = false, rev = 1)
    for {
      (runner, deleted) <- runnerWith(config(deleteDeprecatedProjects = true), List(deprecated, active))
      _                 <- runner.run
      result            <- deleted
    } yield assertEquals(result, List(deprecated.ref -> 2))
  }

  test("fall back gracefully and delete nothing extra when statistics are missing") {
    val project = candidate("nostats", deprecated = false)
    for {
      (runner, deleted) <-
        runnerWith(config(deleteDeprecatedProjects = false), List(project), lastEventTime = _ => IO.none)
      _                 <- runner.run
      result            <- deleted
    } yield assertEquals(result, Nil)
  }

  test("continue deleting the remaining projects when a deletion fails") {
    val first  = candidate("first", deprecated = true)
    val second = candidate("second", deprecated = true)
    for {
      (runner, deleted) <-
        runnerWith(config(deleteDeprecatedProjects = true), List(first, second), failDeletionFor = Set(first.ref))
      _                 <- runner.run
      result            <- deleted
    } yield assertEquals(result, List(second.ref -> second.rev))
  }
}
