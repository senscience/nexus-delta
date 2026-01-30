package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.OrganizationIsDeprecated
import ai.senscience.nexus.delta.sdk.projects.FetchContext.ProjectStatus
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectIsDeprecated, ProjectIsMarkedForDeletion, ProjectNotFound}
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO

class FetchContextSuite extends NexusSuite {

  private val deprecatedOrg     = ProjectRef.unsafe("deprecated-org", "proj")
  private val activeProject     = ProjectRef.unsafe("org", "proj")
  private val deletedProject    = ProjectRef.unsafe("org", "deleted")
  private val deprecatedProject = ProjectRef.unsafe("org", "deprecated")
  private val unknownProject    = ProjectRef.unsafe("org", "xxx")

  private def projectContext(project: ProjectRef) =
    ProjectContext.unsafe(ApiMappings.empty, iri"$project", iri"$project", enforceSchema = false)

  private val deprecatedOrgContext     = projectContext(deprecatedOrg)
  private val activeProjectContext     = projectContext(activeProject)
  private val deletedProjectContext    = projectContext(deletedProject)
  private val deprecatedProjectContext = projectContext(deprecatedProject)

  private def fetchProjectStatus(ref: ProjectRef): IO[Option[ProjectStatus]] = ref match {
    case `deprecatedOrg`     => IO.some(ProjectStatus(true, false, false, deprecatedOrgContext))
    case `activeProject`     => IO.some(ProjectStatus(false, false, false, activeProjectContext))
    case `deletedProject`    => IO.some(ProjectStatus(false, false, true, deletedProjectContext))
    case `deprecatedProject` => IO.some(ProjectStatus(false, true, false, deprecatedProjectContext))
    case _                   => IO.none
  }

  private def fetchContext = FetchContext(
    ApiMappings.empty,
    (project: ProjectRef) => fetchProjectStatus(project)
  )

  // Active project
  test("Successfully get a context for an active project on read/create/modify") {
    fetchContext.onRead(activeProject).assertEquals(activeProjectContext) >>
      fetchContext.onCreate(activeProject).assertEquals(activeProjectContext) >>
      fetchContext.onModify(activeProject).assertEquals(activeProjectContext)
  }

  // Deleted project
  test("Fail getting a context for a project marked as deleted on read/create/modify") {
    val expectedError = ProjectIsMarkedForDeletion(deletedProject)

    fetchContext.onRead(deletedProject).interceptEquals(expectedError) >>
      fetchContext.onCreate(deletedProject).interceptEquals(expectedError) >>
      fetchContext.onModify(deletedProject).interceptEquals(expectedError)
  }

  // Unknown project
  test("Fail getting a context for a unknown project on read/create/modify") {
    val expectedError = ProjectNotFound(unknownProject)

    fetchContext.onRead(unknownProject).interceptEquals(expectedError) >>
      fetchContext.onCreate(unknownProject).interceptEquals(expectedError) >>
      fetchContext.onModify(unknownProject).interceptEquals(expectedError)
  }

  // Deprecated project
  test("Successfully get a context for a deprecated project on read") {
    fetchContext
      .onRead(deprecatedProject)
      .assertEquals(deprecatedProjectContext)
  }

  test("Fail getting a context for a deprecated project on create/modify") {
    val expectedError = ProjectIsDeprecated(deprecatedProject)

    fetchContext.onCreate(deprecatedProject).interceptEquals(expectedError) >>
      fetchContext.onModify(deprecatedProject).interceptEquals(expectedError)
  }

  // Deprecated org
  test("Successfully get a context for a deprecated org on read") {
    fetchContext
      .onRead(deprecatedOrg)
      .assertEquals(deprecatedOrgContext)
  }

  test("Fail getting a context for a deprecated project on create/modify") {
    val expectedError = OrganizationIsDeprecated(deprecatedOrg.organization)

    fetchContext.onCreate(deprecatedOrg).interceptEquals(expectedError) >>
      fetchContext.onModify(deprecatedOrg).interceptEquals(expectedError)
  }

}
