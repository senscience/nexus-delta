package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryClientDummy
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.SparqlJsonLd
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.ViewIsDeprecated
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.{AggregateBlazegraphViewValue, IndexingBlazegraphViewValue}
import ai.senscience.nexus.delta.plugins.blazegraph.model.defaultViewId
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Group, User}
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptySet
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all.*
import io.circe.syntax.EncoderOps
import munit.{AnyFixture, Location}

class BlazegraphViewsQuerySuite extends NexusSuite with ConfigFixtures with Fixtures {

  private given UUIDF = UUIDF.random

  private val realm             = Label.unsafe("myrealm")
  private given aliceUser: User = User("Alice", realm)
  private given alice: Caller   = Caller(aliceUser, Set(User("Alice", realm), Group("users", realm)))
  private val bob: Caller       = Caller(User("Bob", realm), Set(User("Bob", realm), Group("users", realm)))
  private val anon: Caller      = Caller(Anonymous, Set(Anonymous))

  private val project1     = ProjectGen.project("org", "proj")
  private val project2     = ProjectGen.project("org2", "proj2")
  private val fetchContext = FetchContextDummy(List(project1, project2))

  private val queryPermission = Permission.unsafe("views/query")

  private val client = new SparqlQueryClientDummy(sparqlJsonLd = _.asJson)

  private def aclCheck = AclSimpleCheck(
    (alice.subject, AclAddress.Project(project1.ref), Set(queryPermission)),
    (bob.subject, AclAddress.Root, Set(queryPermission)),
    (Anonymous, AclAddress.Project(project2.ref), Set(queryPermission))
  )

  private def createBlazegraphViews(xas: Transactors) = BlazegraphViews(
    fetchContext,
    ResolverContextResolution(rcr),
    alwaysValidate,
    _ => IO.unit,
    eventLogConfig,
    "prefix",
    xas,
    clock
  )

  private def viewsAndQuery: Resource[IO, (BlazegraphViews, BlazegraphViewsQuery)] =
    for {
      xas   <- Doobie.resourceDefault
      views <- Resource.eval(createBlazegraphViews(xas))
      acls  <- Resource.eval(aclCheck)
      query  = BlazegraphViewsQuery(acls, views, client, "prefix", xas)
    } yield (views, query)

  private val fixture = ResourceSuiteLocalFixture("this", viewsAndQuery)

  override def munitFixtures: Seq[AnyFixture[?]] = List(fixture)

  private lazy val (views, viewsQuery) = fixture()

  private val defaultView         = ViewRef(project1.ref, defaultViewId)
  private val view1Proj1          = ViewRef(project1.ref, nxv + "view1Proj1")
  private val view2Proj1          = ViewRef(project1.ref, nxv + "view2Proj1")
  private val view1Proj2          = ViewRef(project2.ref, nxv + "view1Proj2")
  private val view2Proj2          = ViewRef(project2.ref, nxv + "view2Proj2")
  private val deprecatedViewProj1 = ViewRef(project1.ref, nxv + "deprecatedViewProj1")

  private val indexingViews = List(defaultView, view1Proj1, view2Proj1, view1Proj2, view2Proj2)

  // Aggregates all views of project1
  private val aggView1Proj1      = ViewRef(project1.ref, nxv + "aggView1Proj1")
  private val aggView1Proj1Views = AggregateBlazegraphViewValue(
    Some("aggregateView1"),
    Some("aggregates views from project1"),
    NonEmptySet.of(view1Proj1, view2Proj1)
  )

  // Aggregates view1 of project2, references an aggregated view on project 2 and references the previous aggregate which aggregates all views of project1
  private val aggView1Proj2      = ViewRef(project2.ref, nxv + "aggView1Proj2")
  private val aggView1Proj2Views = AggregateBlazegraphViewValue(
    Some("aggregateView2"),
    Some("aggregate view1proj2 and aggregate of project1"),
    NonEmptySet.of(view1Proj2, aggView1Proj1)
  )

  // Aggregates view2 of project2 and references aggView1Proj2
  private val aggView2Proj2      = ViewRef(project2.ref, nxv + "aggView2Proj2")
  private val aggView2Proj2Views = AggregateBlazegraphViewValue(
    Some("aggregateView3"),
    Some("aggregate view2proj2 and aggregateView2"),
    NonEmptySet.of(view2Proj2, aggView1Proj2)
  )

  test("Create the indexing views") {
    indexingViews.traverse { v =>
      views.create(v.viewId, v.project, IndexingBlazegraphViewValue())
    }
  }

  test("Create the deprecated view") {
    views.create(deprecatedViewProj1.viewId, deprecatedViewProj1.project, IndexingBlazegraphViewValue()) >>
      views.deprecate(deprecatedViewProj1.viewId, deprecatedViewProj1.project, 1)
  }

  test("Create the aggregate views") {
    views.create(aggView1Proj1.viewId, aggView1Proj1.project, aggView1Proj1Views) >>
      views.create(aggView1Proj2.viewId, aggView1Proj2.project, aggView1Proj2Views) >>
      views.create(aggView2Proj2.viewId, aggView2Proj2.project, aggView2Proj2Views)
  }

  test("Create the cycle between project2 aggregate views ") {
    val newValue = AggregateBlazegraphViewValue(
      Some("name1"),
      Some("desc1"),
      NonEmptySet.of(view1Proj1, view2Proj1, aggView2Proj2)
    )
    views.update(aggView1Proj1.viewId, aggView1Proj1.project, 1, newValue)
  }

  private val constructQuery = SparqlConstructQuery.unsafe("CONSTRUCT {?s ?p ?o} WHERE { ?s ?p ?o }")

  private def getNamespaces(viewRefs: ViewRef*) =
    viewRefs
      .traverse { view =>
        views.fetchIndexingView(view.viewId, view.project).map(_.namespace)
      }
      .map(_.toSet)

  private def assertNamespaceAccess(view: ViewRef, caller: Caller, expectedNamespaces: Set[String])(using Location) =
    viewsQuery
      .query(view.viewId, view.project, constructQuery, SparqlJsonLd)(using caller)
      .flatMap { response =>
        IO.fromEither(response.value.as[Set[String]])
      }
      .assertEquals(expectedNamespaces)

  test("Query an indexed view") {
    getNamespaces(view1Proj1).flatMap { expectedNamespaces =>
      assertNamespaceAccess(view1Proj1, alice, expectedNamespaces)
    }
  }

  test("Query an indexed view without permissions") {
    val view = view1Proj1
    viewsQuery
      .query(view.viewId, view.project, constructQuery, SparqlJsonLd)(using anon)
      .intercept[AuthorizationFailed]
  }

  test("Query a deprecated view") {
    val view = deprecatedViewProj1
    viewsQuery
      .query(view.viewId, view.project, constructQuery, SparqlJsonLd)
      .intercept[ViewIsDeprecated]
  }

  test("Query an aggregate view with full permissions") {
    // aggView1Proj2 points directly or via other aggregate views to all indexing views but the default one
    val views = List(view1Proj1, view2Proj1, view1Proj2, view2Proj2)
    getNamespaces(views*).flatMap { expectedNamespaces =>
      assertNamespaceAccess(aggView1Proj2, bob, expectedNamespaces)
    }
  }

  test("Query an aggregated view without permissions in some projects") {
    // Alice has only access to proj1
    val views = List(view1Proj1, view2Proj1)
    getNamespaces(views*).flatMap { expectedNamespaces =>
      assertNamespaceAccess(aggView1Proj2, alice, expectedNamespaces)
    }
  }

}
