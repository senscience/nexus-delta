package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schema, xsd}
import ai.senscience.nexus.delta.sdk.generators.{OrganizationGen, ProjectGen}
import ai.senscience.nexus.delta.sdk.organizations.FetchActiveOrganization
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.{OrganizationIsDeprecated, OrganizationNotFound}
import ai.senscience.nexus.delta.sdk.projects.Projects.evaluate
import ai.senscience.nexus.delta.sdk.projects.model.ProjectCommand.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectEvent.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.*
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, PrefixIri, ProjectFields}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}

import java.time.Instant

class ProjectsSuite extends NexusSuite {

  private val epoch = Instant.EPOCH
  private val time2 = Instant.ofEpochMilli(10L)
  private val am    = ApiMappings("xsd" -> xsd.base, "Person" -> schema.Person)
  private val base  = PrefixIri.unsafe(iri"http://example.com/base/")
  private val vocab = PrefixIri.unsafe(iri"http://example.com/vocab/")
  private val org1  = OrganizationGen.state("org", 1)
  private val org2  = OrganizationGen.state("org2", 1, deprecated = true)
  private val state = ProjectGen.state(
    "org",
    "proj",
    1,
    orgUuid = org1.uuid,
    description = Some("desc"),
    mappings = am,
    base = base.value,
    vocab = vocab.value,
    enforceSchema = true
  )

  private val deprecatedState               = state.copy(deprecated = true)
  private val label                         = state.label
  private val uuid                          = state.uuid
  private val orgLabel                      = state.organizationLabel
  private val orgUuid                       = state.organizationUuid
  private val desc                          = state.description
  private val desc2                         = Some("desc2")
  private val org2Label                     = org2.label
  private val subject                       = User("myuser", label)
  private val orgs: FetchActiveOrganization = {
    case `orgLabel`  => IO.pure(org1.toResource.value)
    case `org2Label` => IO.raiseError(OrganizationIsDeprecated(org2Label))
    case label       => IO.raiseError(OrganizationNotFound(label))
  }

  private val ref              = ProjectRef(orgLabel, label)
  private val ref2             = ProjectRef(org2Label, label)
  private val ref2IsReferenced = ProjectIsReferenced(ref, Map(ref -> Set(nxv + "ref1")))

  private val createdProjects                  = Ref.unsafe[IO, Set[ProjectRef]](Set.empty)
  private def onCreateRef(project: ProjectRef) = createdProjects.update(_ + project)

  private val validateDeletion: ValidateProjectDeletion = {
    case `ref`  => IO.unit
    case `ref2` => IO.raiseError(ref2IsReferenced)
    case _      => IO.raiseError(new IllegalArgumentException(s"Only '$ref' and '$ref2' are expected here"))
  }

  private given UUIDF = UUIDF.fixed(uuid)

  private val eval   = evaluate(orgs, onCreateRef, validateDeletion, clock)(_, _)
  private val fields = ProjectFields(desc, am, base, vocab, enforceSchema = true)

  // -----------------------------------------
  // Successful events
  // -----------------------------------------

  test("Create a new create event") {
    val expectedEvent =
      ProjectCreated(label, uuid, orgLabel, orgUuid, 1, desc, am, base, vocab, enforceSchema = true, epoch, subject)
    eval(None, CreateProject(ref, fields, subject)).assertEquals(expectedEvent) >>
      createdProjects.get.map(_.contains(ref))
  }

  test("Create a new update event") {
    val expectedEvent =
      ProjectUpdated(label, uuid, orgLabel, orgUuid, 2, desc, am, base, vocab, enforceSchema = true, epoch, subject)
    eval(Some(state), UpdateProject(ref, fields, 1, subject)).assertEquals(expectedEvent)
  }

  test("Create a new deprecate event") {
    val expectedEvent = ProjectDeprecated(label, uuid, orgLabel, orgUuid, 2, epoch, subject)
    eval(Some(state), DeprecateProject(ref, 1, subject)).assertEquals(expectedEvent)
  }

  test("Create a new undeprecation event") {
    val expectedEvent = ProjectUndeprecated(label, uuid, orgLabel, orgUuid, 2, epoch, subject)
    eval(Some(deprecatedState), UndeprecateProject(ref, 1, subject)).assertEquals(expectedEvent)
  }

  test("Create a new deletion event") {
    val expectedEvent = ProjectMarkedForDeletion(label, uuid, orgLabel, orgUuid, 2, epoch, subject)
    eval(Some(state), DeleteProject(ref, 1, subject)).assertEquals(expectedEvent)
  }

  test("Do not reject deletion with ProjectIsDeprecated") {
    val expectedEvent = ProjectMarkedForDeletion(label, uuid, orgLabel, orgUuid, 2, epoch, subject)
    eval(Some(deprecatedState), DeleteProject(ref, 1, subject)).assertEquals(expectedEvent)
  }

  // -----------------------------------------
  // IncorrectRev rejections
  // -----------------------------------------

  List(
    ("UpdateProject", state, UpdateProject(ref, fields, 2, subject)),
    ("DeprecateProject", state, DeprecateProject(ref, 2, subject)),
    ("UndeprecateProject", deprecatedState, UndeprecateProject(ref, 2, subject))
  ).foreach { case (name, state, cmd) =>
    test(s"$name is rejected with IncorrectRev") {
      eval(Some(state), cmd).intercept[IncorrectRev]
    }
  }

  // -----------------------------------------
  // OrganizationIsDeprecated rejections
  // -----------------------------------------

  List(
    ("CreateProject", None, CreateProject(ref2, fields, subject)),
    ("UpdateProject", Some(state), UpdateProject(ref2, fields, 1, subject)),
    ("DeprecateProject", Some(state), DeprecateProject(ref2, 1, subject)),
    ("UndeprecateProject", Some(deprecatedState), UndeprecateProject(ref2, 1, subject))
  ).foreach { case (name, state, cmd) =>
    test(s"$name is rejected with OrganizationIsDeprecated") {
      eval(state, cmd).interceptEquals(OrganizationIsDeprecated(ref2.organization))
    }
  }

  // -----------------------------------------
  // OrganizationNotFound rejections
  // -----------------------------------------

  private val orgNotFound = ProjectRef(label, Label.unsafe("other"))
  List(
    ("CreateProject", None, CreateProject(orgNotFound, fields, subject)),
    ("UpdateProject", Some(state), UpdateProject(orgNotFound, fields, 1, subject)),
    ("DeprecateProject", Some(state), DeprecateProject(orgNotFound, 1, subject)),
    ("UndeprecateProject", Some(deprecatedState), UndeprecateProject(orgNotFound, 1, subject))
  ).foreach { case (name, state, cmd) =>
    test(s"$name is rejected with OrganizationNotFound") {
      eval(state, cmd).interceptEquals(OrganizationNotFound(label))
    }
  }

  // -----------------------------------------
  // ProjectIsDeprecated rejections
  // -----------------------------------------

  List(
    ("UpdateProject", UpdateProject(ref, fields, 1, subject)),
    ("DeprecateProject", DeprecateProject(ref, 1, subject))
  ).foreach { case (name, cmd) =>
    test(s"$name is rejected with ProjectIsDeprecated") {
      eval(Some(deprecatedState), cmd).intercept[ProjectIsDeprecated]
    }
  }

  // -----------------------------------------
  // Other rejections
  // -----------------------------------------

  test("UndeprecateProject is rejected with ProjectIsNotDeprecated") {
    eval(Some(state), UndeprecateProject(ref, 1, subject)).intercept[ProjectIsNotDeprecated]
  }

  List(
    ("UpdateProject", UpdateProject(ref, fields, 1, subject)),
    ("DeprecateProject", DeprecateProject(ref, 1, subject)),
    ("DeleteProject", DeleteProject(ref, 1, subject)),
    ("UndeprecateProject", UndeprecateProject(ref, 1, subject))
  ).foreach { case (name, cmd) =>
    test(s"$name is rejected with ProjectNotFound") {
      eval(None, cmd).intercept[ProjectNotFound]
    }
  }

  test("CreateProject is rejected with ProjectAlreadyExists") {
    eval(Some(state), CreateProject(ref, fields, subject)).intercept[ProjectAlreadyExists]
  }

  test("DeleteProject is rejected with ProjectIsReferenced") {
    eval(Some(state), DeleteProject(ref2, 1, subject)).interceptEquals(ref2IsReferenced)
  }

  // -----------------------------------------
  // State transitions (next function)
  // -----------------------------------------

  private val next = Projects.next

  test("next: ProjectCreated from None") {
    val event    =
      ProjectCreated(label, uuid, orgLabel, orgUuid, 1, desc, am, base, vocab, enforceSchema = true, time2, subject)
    val expected = state.copy(createdAt = time2, createdBy = subject, updatedAt = time2, updatedBy = subject)
    assertEquals(next(None, event), Some(expected))
  }

  test("next: ProjectCreated from existing state") {
    val event =
      ProjectCreated(label, uuid, orgLabel, orgUuid, 1, desc, am, base, vocab, enforceSchema = true, time2, subject)
    assertEquals(next(Some(state), event), None)
  }

  test("next: ProjectUpdated from None") {
    val event = ProjectUpdated(
      label,
      uuid,
      orgLabel,
      orgUuid,
      2,
      desc2,
      ApiMappings.empty,
      base,
      vocab,
      enforceSchema = false,
      time2,
      subject
    )
    assertEquals(next(None, event), None)
  }

  test("next: ProjectUpdated from existing state") {
    val event    = ProjectUpdated(
      label,
      uuid,
      orgLabel,
      orgUuid,
      2,
      desc2,
      ApiMappings.empty,
      base,
      vocab,
      enforceSchema = false,
      time2,
      subject
    )
    val expected = state.copy(
      rev = 2,
      description = desc2,
      apiMappings = ApiMappings.empty,
      enforceSchema = false,
      updatedAt = time2,
      updatedBy = subject
    )
    assertEquals(next(Some(state), event), Some(expected))
  }

  test("next: ProjectDeprecated from None") {
    val event = ProjectDeprecated(label, uuid, orgLabel, orgUuid, 2, time2, subject)
    assertEquals(next(None, event), None)
  }

  test("next: ProjectDeprecated from existing state") {
    val event    = ProjectDeprecated(label, uuid, orgLabel, orgUuid, 2, time2, subject)
    val expected = state.copy(rev = 2, deprecated = true, updatedAt = time2, updatedBy = subject)
    assertEquals(next(Some(state), event), Some(expected))
  }

  test("next: ProjectMarkedForDeletion from None") {
    val event = ProjectMarkedForDeletion(label, uuid, orgLabel, orgUuid, 2, time2, subject)
    assertEquals(next(None, event), None)
  }

  test("next: ProjectMarkedForDeletion from existing state") {
    val event    = ProjectMarkedForDeletion(label, uuid, orgLabel, orgUuid, 2, time2, subject)
    val expected = state.copy(rev = 2, markedForDeletion = true, updatedAt = time2, updatedBy = subject)
    assertEquals(next(Some(state), event), Some(expected))
  }

  test("next: ProjectUndeprecated from None") {
    val event = ProjectUndeprecated(label, uuid, orgLabel, orgUuid, 2, time2, subject)
    assertEquals(next(None, event), None)
  }

  test("next: ProjectUndeprecated from deprecated state") {
    val event    = ProjectUndeprecated(label, uuid, orgLabel, orgUuid, 2, time2, subject)
    val expected = deprecatedState.copy(rev = 2, deprecated = false, updatedAt = time2, updatedBy = subject)
    assertEquals(next(Some(deprecatedState), event), Some(expected))
  }
}
