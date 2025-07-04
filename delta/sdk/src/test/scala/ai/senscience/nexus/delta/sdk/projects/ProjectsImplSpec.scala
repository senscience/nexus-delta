package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.generators.ProjectGen.{defaultApiMappings, projectFromRef, resourceFor}
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.ProjectSearchParams
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
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
import org.scalatest.CancelAfterFailure

import java.util.UUID

class ProjectsImplSpec extends CatsEffectSpec with DoobieScalaTestFixture with CancelAfterFailure with ConfigFixtures {

  implicit private val subject: Subject = Identity.User("user", Label.unsafe("realm"))
  implicit private val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val uuid                  = UUID.randomUUID()
  implicit private val uuidF: UUIDF = UUIDF.fixed(uuid)

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

  private val payload        = ProjectFields(desc, mappings, Some(base), Some(voc))
  private val anotherPayload = ProjectFields(Some("Another project description"), mappings, None, None)

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
      project shouldEqual resourceFor(
        projectFromRef(ref, uuid, orgUuid, markedForDeletion = false, payload),
        1,
        subject
      )
    }

    val anotherProjResource = resourceFor(
      projectFromRef(anotherRef, uuid, orgUuid, markedForDeletion = false, anotherPayload),
      1,
      Identity.Anonymous
    )

    "create another project" in {
      val project = projects.create(anotherRef, anotherPayload)(Identity.Anonymous).accepted
      project shouldEqual anotherProjResource
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

    val newPayload = payload.copy(base = None, description = None)

    "update a project" in {
      projects.update(ref, 1, newPayload).accepted shouldEqual resourceFor(
        projectFromRef(ref, uuid, orgUuid, markedForDeletion = false, newPayload),
        2,
        subject
      )
    }

    "deprecate a project" in {
      projects.deprecate(ref, 2).accepted shouldEqual resourceFor(
        projectFromRef(ref, uuid, orgUuid, markedForDeletion = false, newPayload),
        3,
        subject,
        deprecated = true
      )
    }

    "not update a project if it has been already deprecated " in {
      projects.update(ref, 3, payload).rejectedWith[ProjectRejection] shouldEqual ProjectIsDeprecated(ref)
    }

    "not deprecate a project if it has been already deprecated " in {
      projects.deprecate(ref, 3).rejectedWith[ProjectRejection] shouldEqual ProjectIsDeprecated(ref)
    }

    "delete a project" in {
      projects.delete(ref, 3).accepted shouldEqual resourceFor(
        projectFromRef(ref, uuid, orgUuid, markedForDeletion = true, newPayload),
        4,
        subject,
        deprecated = true,
        markedForDeletion = true
      )
    }

    "not delete a project that has references" in {
      projects.delete(anotherRef, rev = 1).rejected shouldEqual anotherRefIsReferenced
    }

    val resource = resourceFor(
      projectFromRef(ref, uuid, orgUuid, markedForDeletion = true, newPayload),
      4,
      subject,
      deprecated = true,
      markedForDeletion = true
    )

    "fetch a project" in {
      projects.fetch(ref).accepted shouldEqual resource
    }

    "fetch a project with fetchProject" in {
      projects.fetchProject(ref).accepted shouldEqual resource.value
    }

    "fetch a project at a given revision" in {
      projects.fetchAt(ref, 1).accepted shouldEqual
        resourceFor(projectFromRef(ref, uuid, orgUuid, markedForDeletion = false, payload), 1, subject)
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

    "list projects without filters nor pagination" in {
      val results =
        projects.list(FromPagination(0, 10), ProjectSearchParams(filter = _ => IO.pure(true)), order).accepted

      results shouldEqual SearchResults(2L, Vector(resource, anotherProjResource))
    }

    "list projects without filers but paginated" in {
      val results =
        projects.list(FromPagination(0, 1), ProjectSearchParams(filter = _ => IO.pure(true)), order).accepted

      results shouldEqual SearchResults(2L, Vector(resource))
    }

    "list deprecated projects" in {
      val results =
        projects
          .list(
            FromPagination(0, 10),
            ProjectSearchParams(deprecated = Some(true), filter = _ => IO.pure(true)),
            order
          )
          .accepted

      results shouldEqual SearchResults(1L, Vector(resource))
    }

    "list projects from organization org" in {
      val results =
        projects
          .list(
            FromPagination(0, 10),
            ProjectSearchParams(organization = Some(anotherRef.organization), filter = _ => IO.pure(true)),
            order
          )
          .accepted

      results shouldEqual SearchResults(1L, Vector(anotherProjResource))
    }

    "list projects created by Anonymous" in {
      val results =
        projects
          .list(
            FromPagination(0, 10),
            ProjectSearchParams(createdBy = Some(Identity.Anonymous), filter = _ => IO.pure(true)),
            order
          )
          .accepted

      results shouldEqual SearchResults(1L, Vector(anotherProjResource))
    }

    "run the initializer upon project creation" in {
      val projectRef                    = ProjectRef.unsafe("org", genString())
      val createProjects                = Ref.unsafe[IO, Set[ProjectRef]](Set.empty)
      def onCreate(project: ProjectRef) = createProjects.update(_ + project)

      val initializerWasExecuted = Ref.unsafe[IO, Boolean](false)
      val projectInitializer     = new ScopeInitializer {
        override def initializeOrganization(organizationResource: OrganizationResource)(implicit
            caller: Subject
        ): IO[Unit] =
          IO.unit

        override def initializeProject(project: ProjectRef)(implicit caller: Subject): IO[Unit] =
          initializerWasExecuted.set(true)
      }
      // format: off
      val projects = ProjectsImpl(fetchOrg,onCreate, validateDeletion, projectInitializer, defaultApiMappings, eventLogConfig, xas, clock)
      // format: on

      projects.create(projectRef, payload)(Identity.Anonymous).accepted
      initializerWasExecuted.get.accepted shouldEqual true
      createProjects.get.accepted should contain(projectRef)
    }

  }
}
