package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.generators.ProjectGen.defaultApiMappings
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.ProjectSearchParams
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.organizations.FetchActiveOrganization
import ai.senscience.nexus.delta.sdk.organizations.model.Organization
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.{OrganizationIsDeprecated, OrganizationNotFound}
import ai.senscience.nexus.delta.sdk.projects.model.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}
import munit.{AnyFixture, Location}

import java.util.UUID

class ProjectsImplSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private given subject: Subject = Identity.User("user", Label.unsafe("realm"))

  private val uuid    = UUID.randomUUID()
  private given UUIDF = UUIDF.fixed(uuid)

  private val orgUuid = UUID.randomUUID()

  private val desc = Some("Project description")

  private val mappings = ApiMappings(
    Map(
      "nxv" -> iri"https://localhost/nexus/vocabulary/",
      "rdf" -> iri"http://localhost/1999/02/22-rdf-syntax-ns#type"
    )
  )
  private val base     = PrefixIri.unsafe(iri"https://localhost/base/")
  private val voc      = PrefixIri.unsafe(iri"https://localhost/voc/")

  private val payload = ProjectFields(desc, mappings, base, voc, enforceSchema = false)

  private val org1          = Label.unsafe("org")
  private val org2          = Label.unsafe("org2")
  private val orgDeprecated = Label.unsafe("orgDeprecated")

  private val order = ResourceF.sortBy[Project]("_label").get

  private def fetchOrg: FetchActiveOrganization = {
    case `org1`          => IO.pure(Organization(org1, orgUuid, None))
    case `org2`          => IO.pure(Organization(org2, orgUuid, None))
    case `orgDeprecated` => IO.raiseError(OrganizationIsDeprecated(orgDeprecated))
    case other           => IO.raiseError(OrganizationNotFound(other))
  }

  private val ref: ProjectRef        = ProjectRef.unsafe("org", "proj")
  private val anotherRef: ProjectRef = ProjectRef.unsafe("org2", "proj2")
  private val anotherRefIsReferenced = ProjectIsReferenced(ref, Map(ref -> Set(nxv + "ref1")))

  private val validateDeletion: ValidateProjectDeletion = {
    case `ref`        => IO.unit
    case `anotherRef` => IO.raiseError(anotherRefIsReferenced)
    case _            => IO.raiseError(new IllegalArgumentException(s"Only '$ref' and '$anotherRef' are expected here"))
  }

  private lazy val xas = doobie()

  private def initProjects(onCreate: ProjectRef => IO[Unit], scopeInitializer: ScopeInitializer) =
    ProjectsImpl(fetchOrg, onCreate, validateDeletion, scopeInitializer, defaultApiMappings, eventLogConfig, xas, clock)

  private lazy val projects = initProjects(_ => IO.unit, ScopeInitializer.noop)

  private def assertResults(results: SearchResults[ProjectResource], expectedTotal: Long, expectedRefs: ProjectRef*)(
      using Location
  ): Unit = {
    assertEquals(results.total, expectedTotal)
    assertEquals(results.results.map(_.source.value.ref), expectedRefs)
  }

  test("Create a project") {
    projects.create(ref, payload).map(_.rev).assertEquals(1)
  }

  test("Create another project") {
    val anotherPayload = payload.copy(description = Some("Another project"))
    projects.create(anotherRef, anotherPayload)(using Identity.Anonymous).map(_.rev).assertEquals(1)
  }

  test("Not create a project if it already exists") {
    projects.create(ref, payload).interceptEquals(ProjectAlreadyExists(ref))
  }

  test("Not create a project if its organization is deprecated") {
    val ref = ProjectRef.unsafe("orgDeprecated", "proj")
    projects.create(ref, payload).interceptEquals(OrganizationIsDeprecated(ref.organization))
  }

  test("Not update a project if it doesn't exist") {
    val ref = ProjectRef.unsafe("org", "unknown")
    projects.update(ref, 1, payload).interceptEquals(ProjectNotFound(ref))
  }

  test("Not update a project if a wrong revision is provided") {
    projects.update(ref, 3, payload).interceptEquals(IncorrectRev(3, 1))
  }

  test("Not deprecate a project if it doesn't exist") {
    val ref = ProjectRef.unsafe("org", "unknown")
    projects.deprecate(ref, 1).interceptEquals(ProjectNotFound(ref))
  }

  test("Not deprecate a project if a wrong revision is provided") {
    projects.deprecate(ref, 3).interceptEquals(IncorrectRev(3, 1))
  }

  test("Update a project") {
    val newPayload = payload.copy(description = Some("New description"))
    projects.update(ref, 1, newPayload).map { project =>
      assertEquals(project.rev, 2)
      assertEquals(project.value.description, newPayload.description)
    }
  }

  test("Deprecate a project") {
    projects.deprecate(ref, 2).map { project =>
      assertEquals(project.rev, 3)
      assertEquals(project.deprecated, true)
    }
  }

  test("Not update a project if it has been already deprecated") {
    projects.update(ref, 3, payload).interceptEquals(ProjectIsDeprecated(ref))
  }

  test("Not deprecate a project if it has been already deprecated") {
    projects.deprecate(ref, 3).interceptEquals(ProjectIsDeprecated(ref))
  }

  test("Delete a project") {
    projects.delete(ref, 3).map { project =>
      assertEquals(project.rev, 4)
      assertEquals(project.value.markedForDeletion, true)
    }
  }

  test("Not delete a project that has references") {
    projects.delete(anotherRef, rev = 1).interceptEquals(anotherRefIsReferenced)
  }

  test("Fetch a project") {
    projects.fetch(ref).map(_.rev).assertEquals(4)
  }

  test("Fetch a project with fetchProject") {
    projects.fetchProject(ref).map(_.markedForDeletion).assertEquals(true)
  }

  test("Fetch a project at a given revision") {
    projects.fetchAt(ref, 1).map(_.rev).assertEquals(1)
  }

  test("Fail fetching an unknown project") {
    val ref = ProjectRef.unsafe("org", "unknown")
    projects.fetch(ref).intercept[ProjectNotFound]
  }

  test("Fail fetching an unknown project with fetchProject") {
    val ref = ProjectRef.unsafe("org", "unknown")
    projects.fetchProject(ref).interceptEquals(ProjectNotFound(ref))
  }

  test("Fail fetching an unknown project at a given revision") {
    val ref = ProjectRef.unsafe("org", "unknown")
    projects.fetchAt(ref, 42).intercept[ProjectNotFound]
  }

  test("List projects without filters nor pagination") {
    val searchParams = ProjectSearchParams(filter = _ => IO.pure(true))
    projects.list(FromPagination(0, 10), searchParams, order).map { results =>
      assertResults(results, 2L, ref, anotherRef)
    }
  }

  test("List projects without filters but paginated") {
    val searchParams = ProjectSearchParams(filter = _ => IO.pure(true))
    projects.list(FromPagination(0, 1), searchParams, order).map { results =>
      assertResults(results, 2L, ref)
    }
  }

  test("List deprecated projects") {
    val searchParams = ProjectSearchParams(deprecated = Some(true), filter = _ => IO.pure(true))
    projects.list(FromPagination(0, 10), searchParams, order).map { results =>
      assertResults(results, 1L, ref)
    }
  }

  test("List projects from organization org") {
    val searchParams =
      ProjectSearchParams(organization = Some(anotherRef.organization), filter = _ => IO.pure(true))
    projects.list(FromPagination(0, 10), searchParams, order).map { results =>
      assertResults(results, 1L, anotherRef)
    }
  }

  test("List projects created by Anonymous") {
    val searchParams = ProjectSearchParams(createdBy = Some(Identity.Anonymous), filter = _ => IO.pure(true))
    projects.list(FromPagination(0, 10), searchParams, order).map { results =>
      assertResults(results, 1L, anotherRef)
    }
  }

  test("Run the initializer upon project creation") {
    val projectRef                    = ProjectRef.unsafe("org", genString())
    val createProjects                = Ref.unsafe[IO, Set[ProjectRef]](Set.empty)
    def onCreate(project: ProjectRef) = createProjects.update(_ + project)

    val initializerWasExecuted = Ref.unsafe[IO, Boolean](false)
    val projectInitializer     = new ScopeInitializer {
      override def initializeOrganization(organizationResource: OrganizationResource)(using Subject): IO[Unit] =
        IO.unit

      override def initializeProject(project: ProjectRef)(using Subject): IO[Unit] =
        initializerWasExecuted.set(true)
    }

    val projects = initProjects(onCreate, projectInitializer)

    projects.create(projectRef, payload)(using Identity.Anonymous) >>
      initializerWasExecuted.get.assertEquals(true) >>
      createProjects.get.assert(_.contains(projectRef))
  }
}
