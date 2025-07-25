package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.delta.elasticsearch.Fixtures
import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel.IndexGroup
import ai.senscience.nexus.delta.elasticsearch.model.permissions
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.projectionIndex
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeView
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.ElasticSearchProjection
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource.ProjectSource
import ai.senscience.nexus.delta.plugins.search.Search.{ExecuteSearch, ListProjections, TargetProjection}
import ai.senscience.nexus.delta.plugins.search.model.SearchRejection.UnknownSuite
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.IndexingRev
import ai.senscience.nexus.delta.sourcing.model.Identity.{Group, User}
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef, Tags}
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.data.NonEmptyList
import cats.effect.IO
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}
import org.http4s.Query

import java.time.Instant
import java.util.UUID

class SearchSpec extends CatsEffectSpec with CirceLiteral with ConfigFixtures with Fixtures {

  private val realm                  = Label.unsafe("myrealm")
  implicit private val alice: Caller = Caller(User("Alice", realm), Set(User("Alice", realm), Group("users", realm)))
  private val bob: Caller            = Caller(User("Bob", realm), Set(User("Bob", realm), Group("users", realm)))

  private val project1        = ProjectRef.unsafe("org", "proj")
  private val project2        = ProjectRef.unsafe("org2", "proj2")
  private val queryPermission = Permission.unsafe("views/query")

  private val aclCheck = AclSimpleCheck(
    (alice.subject, AclAddress.Project(project1), Set(queryPermission)),
    (bob.subject, AclAddress.Root, Set(queryPermission))
  ).accepted

  private val mappings = jsonObjectContentOf("test-mapping.json")

  private val prefix = "prefix"

  private val esProjection = ElasticSearchProjection(
    nxv + "searchProjection",
    UUID.randomUUID(),
    IndexingRev.init,
    SparqlConstructQuery.unsafe("CONSTRUCT ..."),
    IriFilter.None,
    IriFilter.None,
    false,
    false,
    false,
    permissions.query,
    Some(IndexGroup.unsafe("search")),
    mappings,
    None,
    ContextObject(JsonObject())
  )

  private val compViewProj1   = CompositeView(
    nxv + "searchView",
    project1,
    NonEmptyList.of(
      ProjectSource(nxv + "searchSource", UUID.randomUUID(), IriFilter.None, IriFilter.None, None, false)
    ),
    NonEmptyList.of(esProjection),
    None,
    UUID.randomUUID(),
    Tags.empty,
    Json.obj(),
    Instant.EPOCH
  )
  private val compViewProj2   = compViewProj1.copy(project = project2, uuid = UUID.randomUUID())
  private val projectionProj1 = TargetProjection(esProjection, compViewProj1)
  private val projectionProj2 = TargetProjection(esProjection, compViewProj2)

  private val projections = Seq(projectionProj1, projectionProj2)

  private val listViews: ListProjections = () => IO.pure(projections)

  private val allSuite   = Label.unsafe("allSuite")
  private val proj1Suite = Label.unsafe("proj1Suite")
  private val proj2Suite = Label.unsafe("proj2Suite")
  private val allSuites  = Map(
    allSuite   -> Set(project1, project2),
    proj1Suite -> Set(project1),
    proj2Suite -> Set(project2)
  )

  private def assertContainProjects(result: Json, projects: ProjectRef*) = {
    val expectedProjections = projections.filter { p => projects.contains(p.view.project) }
    val expectedIndices     = expectedProjections.map { p =>
      projectionIndex(p.projection, p.view.uuid, prefix).value
    }.toSet

    result.as[Set[String]] shouldEqual Right(expectedIndices)
  }

  "Search" should {
    val executeSearch: ExecuteSearch = (_, accessibleIndices, _) => IO.pure(accessibleIndices.asJson)
    lazy val search                  = Search(listViews, aclCheck, executeSearch, prefix, allSuites)

    val matchAll     = jobj"""{"size": 100}"""
    val noParameters = Query.empty

    "search all indices accordingly to Bob's full access" in {
      val results = search.query(matchAll, noParameters)(bob).accepted
      assertContainProjects(results, project1, project2)
    }

    "search only the project 1 index accordingly to Alice's restricted access" in {
      val results = search.query(matchAll, noParameters)(alice).accepted
      assertContainProjects(results, project1)
    }

    "search within an unknown suite" in {
      search.query(Label.unsafe("xxx"), Set.empty, matchAll, noParameters)(bob).rejectedWith[UnknownSuite]
    }

    List(
      (allSuite, List(project1, project2)),
      (proj2Suite, List(project2))
    ).foreach { case (suite, expected) =>
      s"search within suite $suite accordingly to Bob's full access" in {
        val results = search.query(suite, Set.empty, matchAll, noParameters)(bob).accepted
        assertContainProjects(results, expected*)
      }
    }

    List(
      (allSuite, List(project1)),
      (proj2Suite, List.empty)
    ).foreach { case (suite, expected) =>
      s"search within suite $suite accordingly to Alice's restricted access" in {
        val results = search.query(suite, Set.empty, matchAll, noParameters)(alice).accepted
        assertContainProjects(results, expected*)
      }
    }

    "Search on proj2Suite and add project1 as an extra project accordingly to Bob's full access" in {
      val results = search.query(proj2Suite, Set(project1), matchAll, noParameters)(bob).accepted
      assertContainProjects(results, project1, project2)
    }

    "Search on proj1Suite and add project2 as an extra project accordingly to Alice's restricted access" in {
      val results = search.query(proj1Suite, Set(project2), matchAll, noParameters)(alice).accepted
      assertContainProjects(results, project1)
    }

    "Search on proj2Suite and add project1 as an extra project accordingly to Alice's restricted access" in {
      val results = search.query(proj2Suite, Set(project1), matchAll, noParameters)(alice).accepted
      assertContainProjects(results, project1)
    }
  }
}
