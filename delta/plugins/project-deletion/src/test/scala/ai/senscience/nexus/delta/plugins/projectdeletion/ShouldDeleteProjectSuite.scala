package ai.senscience.nexus.delta.plugins.projectdeletion

import ai.senscience.nexus.delta.plugins.projectdeletion.ShouldDeleteProjectSuite.{assertDeleted, assertNotDeleted, configWhere, projectWhere, shouldBeDeleted, ThreeHoursAgo, TwoDaysAgo}
import ai.senscience.nexus.delta.projectdeletion.ShouldDeleteProject
import ai.senscience.nexus.delta.projectdeletion.model.ProjectDeletionConfig
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sdk.ProjectResource
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.projects.model.Project
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ResourceRef
import ai.senscience.nexus.testkit.Generators
import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{Clock, IO}
import munit.{Assertions, CatsEffectAssertions, Location}

import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.matching.Regex

class ShouldDeleteProjectSuite extends NexusSuite {

  test("delete a deprecated project") {
    assertDeleted(
      shouldBeDeleted(
        configWhere(deleteDeprecatedProjects = true),
        projectWhere(deprecated = true)
      )
    )
  }

  test("not delete a non-deprecated project") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(deleteDeprecatedProjects = true),
        projectWhere(deprecated = false)
      )
    )
  }

  test("not delete a deprecated project if the feature is disabled") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(deleteDeprecatedProjects = false),
        projectWhere(deprecated = true)
      )
    )
  }

  test("delete a project which has been inactive too long") {
    assertDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        projectWhere(updatedAt = TwoDaysAgo, lastEventTime = TwoDaysAgo)
      )
    )
  }

  test("not delete a project which has been updated recently") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        projectWhere(updatedAt = ThreeHoursAgo, lastEventTime = TwoDaysAgo)
      )
    )
  }

  test("not delete a project which has recent events") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        projectWhere(updatedAt = TwoDaysAgo, lastEventTime = ThreeHoursAgo)
      )
    )
  }

  test("not delete a project if the org/label does not match the inclusion regex") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours, includedProjects = List("hippocampus.+".r, ".*neuron".r)),
        projectWhere(updatedAt = ThreeHoursAgo, lastEventTime = ThreeHoursAgo, org = "hippocampus", label = "mouse")
      )
    )
  }

  test("not delete a project if the org/label matches the exclusion regex") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours, excludedProjects = List("hippocampus.+".r, ".*neuron".r)),
        projectWhere(updatedAt = ThreeHoursAgo, lastEventTime = ThreeHoursAgo, org = "thalamus", label = "neuron")
      )
    )
  }

  test("not run against already deleted projects") {
    assertNotDeleted(
      shouldBeDeleted(
        configWhere(idleInterval = 24.hours),
        projectWhere(updatedAt = TwoDaysAgo, markedForDeletion = true)
      )
    )
  }
}

object ShouldDeleteProjectSuite extends Assertions with CatsEffectAssertions with Generators with FixedClock {
  case class ProjectFixture(
      deprecated: Boolean,
      updatedAt: Instant,
      lastEventTime: Instant,
      org: String,
      label: String,
      id: Iri,
      markedForDeletion: Boolean
  ) {
    val resource = {
      val project = ProjectGen.project(
        orgLabel = org,
        label = label,
        markedForDeletion = markedForDeletion
      )

      ResourceF[Project](
        id = id,
        access = ResourceAccess.project(project.ref),
        rev = 0,
        types = Set.empty,
        deprecated = deprecated,
        createdAt = Instant.EPOCH,
        createdBy = Anonymous,
        updatedAt = updatedAt,
        updatedBy = Anonymous,
        schema = ResourceRef(schemas.resources),
        value = project
      )
    }
  }

  def projectWhere(
      deprecated: Boolean = false,
      updatedAt: Instant = Instant.now(),
      lastEventTime: Instant = Instant.now(),
      org: String = genId(),
      label: String = genId(),
      id: Iri = nxv + genId(),
      markedForDeletion: Boolean = false
  ) = {
    ProjectFixture(deprecated, updatedAt, lastEventTime, org, label, id, markedForDeletion)
  }

  def genId(length: Int = 15): String =
    genString(length = length, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  def configWhere(
      deleteDeprecatedProjects: Boolean = false,
      idleInterval: FiniteDuration = 1.second,
      includedProjects: List[Regex] = List(".*".r),
      excludedProjects: List[Regex] = Nil
  ): ProjectDeletionConfig = {
    ProjectDeletionConfig(
      idleInterval,
      idleCheckPeriod = 1.day,
      deleteDeprecatedProjects,
      includedProjects,
      excludedProjects
    )
  }

  def addTo(deletedProjects: mutable.Set[ProjectResource]): ProjectResource => IO[Unit] = { pr =>
    IO.delay {
      deletedProjects.add(pr)
      ()
    }
  }

  def assertDeleted(result: IO[Boolean])(implicit loc: Location): IO[Unit] = {
    assertIO[Boolean, Boolean](result, true, "project was not deleted")
  }

  def assertNotDeleted(result: IO[Boolean])(implicit loc: Location): IO[Unit] = {
    assertIO[Boolean, Boolean](result, false, "project was deleted")
  }

  val TwoDaysAgo    = Instant.now().minus(Duration.ofDays(2))
  val ThreeHoursAgo = Instant.now().minus(Duration.ofHours(3))

  override def clock: Clock[IO] = implicitly[Clock[IO]]

  def shouldBeDeleted(
      config: ProjectDeletionConfig,
      project: ProjectFixture
  ): IO[Boolean] = {
    val shouldDeleteProject = ShouldDeleteProject(
      config,
      lastEventTime = (_, _) => IO.pure(project.lastEventTime),
      clock
    )

    shouldDeleteProject(project.resource)
  }
}
