package ai.senscience.nexus.delta.plugins.projectdeletion

import ai.senscience.nexus.delta.plugins.projectdeletion.ShouldDeleteProjectSuite.{assertDeleted, assertNotDeleted, candidate, configWhere, shouldBeDeleted, ThreeHoursAgo, TwoDaysAgo}
import ai.senscience.nexus.delta.projectdeletion.ProjectDeletionRunner.ProjectDeletionCandidate
import ai.senscience.nexus.delta.projectdeletion.ShouldDeleteProject
import ai.senscience.nexus.delta.projectdeletion.model.ProjectDeletionConfig
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.Generators
import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{Clock, IO}
import munit.{Assertions, CatsEffectAssertions, Location}

import java.time.{Duration, Instant}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.matching.Regex

class ShouldDeleteProjectSuite extends NexusSuite {

  test("delete a deprecated project") {
    assertDeleted(
      shouldBeDeleted(
        configWhere(deleteDeprecatedProjects = true),
        candidate(deprecated = true)
      )
    )
  }

  test("not delete a non-deprecated project") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(deleteDeprecatedProjects = true),
        candidate(deprecated = false)
      )
    )
  }

  test("not delete a deprecated project if the feature is disabled") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(deleteDeprecatedProjects = false),
        candidate(deprecated = true)
      )
    )
  }

  test("delete a project which has been inactive too long") {
    assertDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        candidate(updatedAt = TwoDaysAgo),
        lastEventTime = Some(TwoDaysAgo)
      )
    )
  }

  test("not delete a project which has been updated recently") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        candidate(updatedAt = ThreeHoursAgo),
        lastEventTime = Some(TwoDaysAgo)
      )
    )
  }

  test("not delete a project which has recent events") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        candidate(updatedAt = TwoDaysAgo),
        lastEventTime = Some(ThreeHoursAgo)
      )
    )
  }

  test("not delete a project if the org/label does not match the inclusion regex") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours, includedProjects = List("hippocampus.+".r, ".*neuron".r)),
        candidate(updatedAt = ThreeHoursAgo, org = "hippocampus", label = "mouse"),
        lastEventTime = Some(ThreeHoursAgo)
      )
    )
  }

  test("not delete a project if the org/label matches the exclusion regex") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours, excludedProjects = List("hippocampus.+".r, ".*neuron".r)),
        candidate(updatedAt = ThreeHoursAgo, org = "thalamus", label = "neuron"),
        lastEventTime = Some(ThreeHoursAgo)
      )
    )
  }

  test("not delete a project idle by update date when its statistics are missing") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        candidate(updatedAt = TwoDaysAgo),
        lastEventTime = None
      )
    )
  }

  test("not run against already deleted projects") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        candidate(updatedAt = TwoDaysAgo, markedForDeletion = true)
      )
    )
  }
}

object ShouldDeleteProjectSuite extends Assertions with CatsEffectAssertions with Generators with FixedClock {

  def candidate(
      deprecated: Boolean = false,
      updatedAt: Instant = Instant.now(),
      org: String = genId(),
      label: String = genId(),
      markedForDeletion: Boolean = false
  ): ProjectDeletionCandidate =
    ProjectDeletionCandidate(ProjectRef.unsafe(org, label), rev = 0, deprecated, markedForDeletion, updatedAt)

  def genId(length: Int = 15): String =
    genString(length = length, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  def configWhere(
      deleteDeprecatedProjects: Boolean = false,
      idleInterval: FiniteDuration = 1.second,
      includedProjects: List[Regex] = List(".*".r),
      excludedProjects: List[Regex] = Nil
  ): ProjectDeletionConfig =
    ProjectDeletionConfig(
      idleInterval,
      idleCheckPeriod = 1.day,
      deleteDeprecatedProjects,
      includedProjects,
      excludedProjects
    )

  def assertDeleted(result: IO[Boolean])(using Location): IO[Unit] =
    assertIO[Boolean, Boolean](result, true, "Project was not deleted")

  def assertNotDeleted(result: IO[Boolean])(using Location): IO[Unit] =
    assertIO[Boolean, Boolean](result, false, "Project was deleted")

  private val TwoDaysAgo    = Instant.now().minus(Duration.ofDays(2))
  private val ThreeHoursAgo = Instant.now().minus(Duration.ofHours(3))

  override def clock: Clock[IO] = implicitly[Clock[IO]]

  def shouldBeDeleted(
      config: ProjectDeletionConfig,
      candidate: ProjectDeletionCandidate,
      lastEventTime: Option[Instant] = Some(Instant.now())
  ): IO[Boolean] =
    ShouldDeleteProject(config, _ => IO.pure(lastEventTime), clock)(candidate)
}
