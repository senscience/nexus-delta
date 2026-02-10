package ai.senscience.nexus.delta.sdk.organizations

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.sdk.generators.OrganizationGen.{organization, resourceFor}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.OrganizationSearchParams
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.UnscoredSearchResults
import ai.senscience.nexus.delta.sdk.organizations.model.Organization
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.*
import ai.senscience.nexus.delta.sdk.{ConfigFixtures, OrganizationResource, ScopeInitializer}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}
import munit.AnyFixture

import java.util.UUID

class OrganizationsImplSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private val uuid               = UUID.randomUUID()
  private given uuidF: UUIDF     = UUIDF.fixed(uuid)
  private given subject: Subject = Identity.User("user", Label.unsafe("realm"))

  private val description  = Some("my description")
  private val description2 = Some("my other description")
  private val label        = Label.unsafe("myorg")
  private val label2       = Label.unsafe("myorg2")

  private def orgInitializer(wasExecuted: Ref[IO, Boolean]) = new ScopeInitializer {
    override def initializeOrganization(organizationResource: OrganizationResource)(using Subject): IO[Unit] =
      wasExecuted.set(true)

    override def initializeProject(project: ProjectRef)(using Subject): IO[Unit] =
      IO.unit
  }

  private lazy val xas  = doobie()
  private lazy val orgs = OrganizationsImpl(ScopeInitializer.noop, eventLogConfig, xas, clock)

  test("Create an organization") {
    val expected = resourceFor(organization("myorg", uuid, description), 1, subject)
    orgs.create(label, description).assertEquals(expected)
  }

  test("Update an organization") {
    val expected = resourceFor(organization("myorg", uuid, description2), 2, subject)
    orgs.update(label, description2, 1).assertEquals(expected)
  }

  test("Deprecate an organization") {
    val expected = resourceFor(organization("myorg", uuid, description2), 3, subject, deprecated = true)
    orgs.deprecate(label, 2).assertEquals(expected)
  }

  test("Fetch an organization") {
    val expected = resourceFor(organization("myorg", uuid, description2), 3, subject, deprecated = true)
    orgs.fetch(label).assertEquals(expected)
  }

  test("Fetch an organization at specific revision") {
    val expected = resourceFor(organization("myorg", uuid, description), 1, subject)
    orgs.fetchAt(label, 1).assertEquals(expected)
  }

  test("Fail fetching a non existing organization") {
    orgs.fetch(Label.unsafe("non-existing")).intercept[OrganizationNotFound]
  }

  test("Fail fetching a non existing organization at specific revision") {
    orgs.fetchAt(Label.unsafe("non-existing"), 1).intercept[OrganizationNotFound]
  }

  test("Create another organization") {
    orgs.create(label2, None)
  }

  test("List organizations") {
    val allFilter              = OrganizationSearchParams(filter = _ => IO.pure(true))
    val deprecatedAtRev3Filter =
      OrganizationSearchParams(deprecated = Some(true), rev = Some(3), filter = _ => IO.pure(true))
    val order                  = ResourceF.defaultSort[Organization]
    for {
      org1 <- orgs.fetch(label)
      org2 <- orgs.fetch(label2)
      _    <- orgs.list(FromPagination(0, 1), allFilter, order).assertEquals(SearchResults(2L, Seq(org1)))
      _    <- orgs.list(FromPagination(0, 10), allFilter, order).assertEquals(SearchResults(2L, Seq(org1, org2)))
      _    <- orgs.list(FromPagination(0, 10), deprecatedAtRev3Filter, order).assertEquals(SearchResults(1L, Seq(org1)))
    } yield ()
  }

  test("Run the initializer upon organization creation") {
    for {
      ref  <- Ref.of[IO, Boolean](false)
      orgs2 = OrganizationsImpl(orgInitializer(ref), eventLogConfig, xas, clock)
      _    <- orgs2.create(Label.unsafe(genString()), description)
      _    <- ref.get.assertEquals(true)
    } yield ()
  }

  test("Fail to fetch an organization on nonexistent revision") {
    orgs.fetchAt(label, 10).interceptEquals(RevisionNotFound(10, 3))
  }

  test("Fail to create an organization already created") {
    orgs.create(label, None).intercept[OrganizationAlreadyExists]
  }

  test("Fail to update an organization with incorrect rev") {
    orgs.update(label, None, 1).interceptEquals(IncorrectRev(1, 3))
  }

  test("Fail to deprecate an organization with incorrect rev") {
    orgs.deprecate(label, 1).interceptEquals(IncorrectRev(1, 3))
  }

  test("Fail to update a non existing organization") {
    orgs.update(Label.unsafe("other"), None, 1).intercept[OrganizationNotFound]
  }

  test("Fail to deprecate a non existing organization") {
    orgs.deprecate(Label.unsafe("other"), 1).intercept[OrganizationNotFound]
  }

  test("Fail to deprecate an already deprecated organization") {
    orgs.deprecate(label, 3).intercept[OrganizationIsDeprecated]
  }

}
