package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.sdk.error.ServiceError.ScopeInitializationFailed
import ai.senscience.nexus.delta.sdk.generators.{OrganizationGen, ProjectGen}
import ai.senscience.nexus.delta.sdk.organizations.model.Organization
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.OrganizationInitializationFailed
import ai.senscience.nexus.delta.sdk.projects.ScopeInitializationErrorStore
import ai.senscience.nexus.delta.sdk.projects.ScopeInitializationErrorStore.ScopeInitErrorRow
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.ProjectInitializationFailed
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}

import java.time.Instant

class ScopeInitializerSuite extends NexusSuite {

  implicit private val subject: Subject = User("myuser", Label.unsafe("myrealm"))

  private val fail = new ScopeInitialization {
    override def onOrganizationCreation(organization: Organization, subject: Subject): IO[Unit] =
      IO.raiseError(ScopeInitializationFailed("failed during org creation"))
    override def onProjectCreation(project: ProjectRef, subject: Subject): IO[Unit]             =
      IO.raiseError(ScopeInitializationFailed("failed during project creation"))
    override def entityType: EntityType                                                         = EntityType("fail")
  }

  private val projectSignal = "project step executed"
  private val orgSignal     = "org step executed"

  // A mock scope initialization that can be used to assert that init steps were executed
  class MockScopeInitialization extends ScopeInitialization {
    private val wasExecuted                                                                     = Ref.unsafe[IO, String]("")
    override def onOrganizationCreation(organization: Organization, subject: Subject): IO[Unit] =
      wasExecuted.set(orgSignal)
    override def onProjectCreation(project: ProjectRef, subject: Subject): IO[Unit]             =
      wasExecuted.set(projectSignal)
    override def entityType: EntityType                                                         = EntityType("mock")

    def assertOrgInitializationWasExecuted: IO[Unit]     = assertIO(wasExecuted.get, orgSignal)
    def assertProjectInitializationWasExecuted: IO[Unit] = assertIO(wasExecuted.get, projectSignal)
  }

  private val failingScopeInitializer = ScopeInitializer.withoutErrorStore(Set(fail))

  private val org: OrganizationResource =
    OrganizationGen.resourceFor(OrganizationGen.organization("myorg"), 1, subject)
  private val project: ProjectResource  =
    ProjectGen.resourceFor(ProjectGen.project("myorg", "myproject"), 1, subject)

  private def simpleErrorStore(errors: Ref[IO, List[ScopeInitErrorRow]]) = new ScopeInitializationErrorStore {
    override def save(entityType: EntityType, project: ProjectRef, e: ScopeInitializationFailed): IO[Unit] =
      errors.update(
        _ :+ ScopeInitErrorRow(0, entityType, project.organization, project.project, e.getMessage, Instant.EPOCH)
      )

    override def fetch: IO[List[ScopeInitErrorRow]] =
      errors.get

    override def delete(project: ProjectRef): IO[Unit] = IO.unit
  }

  private val projectRef = ProjectRef(Label.unsafe("myorg"), Label.unsafe("myproject"))

  test("A ScopeInitializer should succeed if there are no init steps") {
    ScopeInitializer.noop.initializeOrganization(org) >>
      ScopeInitializer.noop.initializeProject(projectRef)
  }

  test("A ScopeInitializer should fail if there is a failing org init step") {
    failingScopeInitializer
      .initializeOrganization(org)
      .intercept[OrganizationInitializationFailed]
  }

  test("A ScopeInitializer should fail if there is a failing project init step") {
    failingScopeInitializer
      .initializeProject(projectRef)
      .intercept[ProjectInitializationFailed]
  }

  test("The ScopeInitializer should execute the provided init step upon org creation") {
    val init             = new MockScopeInitialization
    val scopeInitializer = ScopeInitializer.withoutErrorStore(Set(init))

    scopeInitializer.initializeOrganization(org) >>
      init.assertOrgInitializationWasExecuted
  }

  test("The ScopeInitializer should execute the provided init step upon project creation") {
    val init             = new MockScopeInitialization
    val scopeInitializer = ScopeInitializer.withoutErrorStore(Set(init))

    scopeInitializer.initializeProject(projectRef) >>
      init.assertProjectInitializationWasExecuted
  }

  test("A failing step should not prevent a successful one to run on org creation") {
    val init             = new MockScopeInitialization
    val scopeInitializer = ScopeInitializer.withoutErrorStore(Set(init, fail))

    scopeInitializer.initializeOrganization(org).intercept[OrganizationInitializationFailed] >>
      init.assertOrgInitializationWasExecuted
  }

  test("A failing step should not prevent a successful one to run on project creation") {
    val init             = new MockScopeInitialization
    val scopeInitializer = ScopeInitializer.withoutErrorStore(Set(fail, init))

    scopeInitializer.initializeProject(projectRef).intercept[ProjectInitializationFailed] >>
      init.assertProjectInitializationWasExecuted
  }

  test("Save an error upon project initialization failure") {
    val errors           = Ref.unsafe[IO, List[ScopeInitErrorRow]](List.empty)
    val scopeInitializer = ScopeInitializer(Set(fail), simpleErrorStore(errors))
    // format: off
    val expectedErrorRow = List(ScopeInitErrorRow(0, EntityType("fail"), org.value.label, project.value.ref.project, "failed during project creation", Instant.EPOCH))
    // format: on

    scopeInitializer.initializeProject(projectRef).intercept[ProjectInitializationFailed] >>
      assertIO(errors.get, expectedErrorRow)
  }

}
