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
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{IncorrectRev, ProjectAlreadyExists, ProjectIsDeprecated, ProjectIsReferenced, ProjectNotFound}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.effect.{IO, Ref}

import java.util.UUID

class ProjectsImplSpec extends CatsEffectSpec with DoobieScalaTestFixture with ConfigFixtures {

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

  private val order = ResourceF.sortBy[Project]("_label").value

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

  private lazy val projects =
    ProjectsImpl(
      fetchOrg,
      _ => IO.unit,
      validateDeletion,
      ScopeInitializer.noop,
      defaultApiMappings,
      eventLogConfig,
      xas,
      clock
    )

  "The Projects operations bundle" should {
    "create a project" in {
      val project = projects.create(ref, payload).accepted
      project.rev shouldEqual 1
    }

    "create another project" in {
      val p       = payload.copy(description = Some("Another project"))
      val project = projects.create(anotherRef, p)(using Identity.Anonymous).accepted
      project.rev shouldEqual 1
    }

    "not create a project if it already exists" in {
      projects.create(ref, payload).rejected shouldEqual ProjectAlreadyExists(ref)
    }

    "not create a project if its organization is deprecated" in {
      val ref = ProjectRef.unsafe("orgDeprecated", "proj")

      projects.create(ref, payload).rejected shouldEqual OrganizationIsDeprecated(ref.organization)
    }

    "not update a project if it doesn't exists" in {
      val ref = ProjectRef.unsafe("org", "unknown")

      projects.update(ref, 1, payload).rejectedWith[ProjectRejection] shouldEqual ProjectNotFound(ref)
    }

    "not update a project if a wrong revision is provided" in {
      projects.update(ref, 3, payload).rejectedWith[ProjectRejection] shouldEqual IncorrectRev(3, 1)
    }

    "not deprecate a project if it doesn't exists" in {
      val ref = ProjectRef.unsafe("org", "unknown")

      projects.deprecate(ref, 1).rejectedWith[ProjectRejection] shouldEqual ProjectNotFound(ref)
    }

    "not deprecate a project if a wrong revision is provided" in {
      projects.deprecate(ref, 3).rejectedWith[ProjectRejection] shouldEqual IncorrectRev(3, 1)
    }

    val newPayload = payload.copy(description = Some("New description"))

    "update a project" in {
      val project = projects.update(ref, 1, newPayload).accepted
      project.rev shouldEqual 2
      project.value.description shouldEqual newPayload.description
    }

    "deprecate a project" in {
      val project = projects.deprecate(ref, 2).accepted
      project.rev shouldEqual 3
      project.deprecated shouldEqual true
    }

    "not update a project if it has been already deprecated " in {
      projects.update(ref, 3, payload).rejectedWith[ProjectRejection] shouldEqual ProjectIsDeprecated(ref)
    }

    "not deprecate a project if it has been already deprecated " in {
      projects.deprecate(ref, 3).rejectedWith[ProjectRejection] shouldEqual ProjectIsDeprecated(ref)
    }

    "delete a project" in {
      val project = projects.delete(ref, 3).accepted
      project.rev shouldEqual 4
      project.value.markedForDeletion shouldEqual true
    }

    "not delete a project that has references" in {
      projects.delete(anotherRef, rev = 1).rejected shouldEqual anotherRefIsReferenced
    }

    "fetch a project" in {
      val project = projects.fetch(ref).accepted
      project.rev shouldEqual 4
    }

    "fetch a project with fetchProject" in {
      val project = projects.fetchProject(ref).accepted
      project.markedForDeletion shouldEqual true
    }

    "fetch a project at a given revision" in {
      val project = projects.fetchAt(ref, 1).accepted
      project.rev shouldEqual 1
    }

    "fail fetching an unknown project" in {
      val ref = ProjectRef.unsafe("org", "unknown")
      projects.fetch(ref).rejectedWith[ProjectNotFound]
    }

    "fail fetching an unknown project with fetchProject" in {
      val ref = ProjectRef.unsafe("org", "unknown")

      projects.fetchProject(ref).rejected shouldEqual
        ProjectNotFound(ref)
    }

    "fail fetching an unknown project at a given revision" in {
      val ref = ProjectRef.unsafe("org", "unknown")

      projects.fetchAt(ref, 42).rejectedWith[ProjectNotFound]
    }

    def extractProjectRefs(results: SearchResults[ProjectResource]) =
      results.results.map(_.source.value.ref)

    "list projects without filters nor pagination" in {
      val searchParams = ProjectSearchParams(filter = _ => IO.pure(true))
      val results      = projects.list(FromPagination(0, 10), searchParams, order).accepted

      results.total shouldEqual 2L
      extractProjectRefs(results) should contain inOrderOnly (ref, anotherRef)
    }

    "list projects without filers but paginated" in {
      val searchParams = ProjectSearchParams(filter = _ => IO.pure(true))
      val results      = projects.list(FromPagination(0, 1), searchParams, order).accepted

      results.total shouldEqual 2L
      extractProjectRefs(results) should contain only ref
    }

    "list deprecated projects" in {
      val searchParams = ProjectSearchParams(deprecated = Some(true), filter = _ => IO.pure(true))
      val results      = projects.list(FromPagination(0, 10), searchParams, order).accepted

      results.total shouldEqual 1L
      extractProjectRefs(results) should contain only ref
    }

    "list projects from organization org" in {
      val searchParams = ProjectSearchParams(organization = Some(anotherRef.organization), filter = _ => IO.pure(true))
      val results      = projects.list(FromPagination(0, 10), searchParams, order).accepted

      results.total shouldEqual 1L
      extractProjectRefs(results) should contain only anotherRef
    }

    "list projects created by Anonymous" in {
      val searchParams = ProjectSearchParams(createdBy = Some(Identity.Anonymous), filter = _ => IO.pure(true))
      val results      = projects.list(FromPagination(0, 10), searchParams, order).accepted

      results.total shouldEqual 1L
      extractProjectRefs(results) should contain only anotherRef
    }

    "run the initializer upon project creation" in {
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
      // format: off
      val projects = ProjectsImpl(fetchOrg,onCreate, validateDeletion, projectInitializer, defaultApiMappings, eventLogConfig, xas, clock)
      // format: on

      projects.create(projectRef, payload)(using Identity.Anonymous).accepted
      initializerWasExecuted.get.accepted shouldEqual true
      createProjects.get.accepted should contain(projectRef)
    }

  }
}
