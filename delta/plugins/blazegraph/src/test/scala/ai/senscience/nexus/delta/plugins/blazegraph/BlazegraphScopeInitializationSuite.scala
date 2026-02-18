package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.blazegraph.model.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphView.{AggregateBlazegraphView, IndexingBlazegraphView}
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.ViewNotFound
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schema as schemaorg}
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.{ConfigFixtures, Defaults}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import munit.AnyFixture

import java.util.UUID

class BlazegraphScopeInitializationSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures with Fixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private val uuid    = UUID.randomUUID()
  private given UUIDF = UUIDF.fixed(uuid)

  private val prefix = "prefix"

  private val saRealm: Label       = Label.unsafe("service-accounts")
  private val usersRealm: Label    = Label.unsafe("users")
  private given sa: ServiceAccount = ServiceAccount(User("nexus-sa", saRealm))
  private given bob: Subject       = User("bob", usersRealm)

  private val am       = ApiMappings("nxv" -> nxv.base, "Person" -> schemaorg.Person)
  private val projBase = nxv.base
  private val project  =
    ProjectGen.project("org", "project", uuid = uuid, orgUuid = uuid, base = projBase, mappings = am)

  private val fetchContext = FetchContextDummy(List(project))

  private lazy val views: BlazegraphViews = BlazegraphViews(
    fetchContext,
    ResolverContextResolution(rcr),
    alwaysValidate,
    _ => IO.unit,
    eventLogConfig,
    prefix,
    doobie(),
    clock
  ).accepted

  private val defaultViewName        = "defaultName"
  private val defaultViewDescription = "defaultDescription"
  private val defaults               = Defaults(defaultViewName, defaultViewDescription)

  private lazy val init = new BlazegraphScopeInitialization(views, sa, defaults)

  test("Create a default SparqlView on newly created project") {
    views.fetch(defaultViewId, project.ref).intercept[ViewNotFound] >>
      init.onProjectCreation(project.ref, bob) >>
      views.fetch(defaultViewId, project.ref).map { r =>
        r.value match {
          case v: IndexingBlazegraphView  =>
            assertEquals(v.resourceSchemas, IriFilter.None)
            assertEquals(v.resourceTypes, IriFilter.None)
            assertEquals(v.resourceTag, None)
            assertEquals(v.includeDeprecated, true)
            assertEquals(v.includeMetadata, true)
            assertEquals(v.permission, permissions.query)
            assertEquals(v.name, Some(defaultViewName))
            assertEquals(v.description, Some(defaultViewDescription))
          case _: AggregateBlazegraphView => fail("Expected an IndexingBlazegraphView to be created")
        }
        assertEquals(r.rev, 1)
        assertEquals(r.createdBy, sa.caller.subject)
      }
  }

  test("Not create a default SparqlView if one already exists") {
    views.fetch(defaultViewId, project.ref).map(_.rev).assertEquals(1) >>
      init.onProjectCreation(project.ref, bob) >>
      views.fetch(defaultViewId, project.ref).map(_.rev).assertEquals(1)
  }
}
