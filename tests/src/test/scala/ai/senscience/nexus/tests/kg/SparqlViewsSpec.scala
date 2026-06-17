package ai.senscience.nexus.tests.kg

import ai.senscience.nexus.delta.kernel.utils.UrlUtils
import ai.senscience.nexus.tests.BaseIntegrationSpec
import ai.senscience.nexus.tests.Identity.Anonymous
import ai.senscience.nexus.tests.Identity.views.ScoobyDoo
import ai.senscience.nexus.tests.Optics.*
import ai.senscience.nexus.tests.StatisticsAssertions.{expectEmptyStats, expectStats}
import ai.senscience.nexus.tests.iam.types.Permission.{Organizations, Views}
import cats.effect.IO
import cats.implicits.*
import io.circe.Json
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.scalatest.Assertion

class SparqlViewsSpec extends BaseIntegrationSpec {

  private val orgId  = genId()
  private val projId = genId()
  val project1       = s"$orgId/$projId"

  private val projId2 = genId()
  val project2        = s"$orgId/$projId2"

  private val projects = List(project1, project2)

  val idBase = "https://test.bbp.epfl.ch/"

  /** Base IRI shared by the patched-cell instance fixtures. */
  private val patchedCellBase = "https://bbp.epfl.ch/nexus/v0/data/bbp/experiment/patchedcell/v0.1.0/"

  /** The `@id` declared in a resource payload. */
  private def resourceId(payload: Json): String = `@id`.getOption(payload).value

  /** The local part of a patched-cell resource `@id`, used to build `patchedcell:<suffix>` paths. */
  private def patchedCellId(payload: Json): String = resourceId(payload).stripPrefix(patchedCellBase)

  override def beforeAll(): Unit = {
    super.beforeAll()
    val setPermissions = for {
      _ <- aclDsl.addPermission("/", ScoobyDoo, Organizations.Create)
      _ <- aclDsl.addPermissionAnonymous(s"/$project2", Views.Query)
    } yield succeed

    val createProjects = for {
      _ <- adminDsl.createOrganization(orgId, orgId, ScoobyDoo)
      _ <- adminDsl.createProjectWithName(orgId, projId, name = project1, ScoobyDoo)
      _ <- adminDsl.createProjectWithName(orgId, projId2, name = project2, ScoobyDoo)
    } yield succeed

    (setPermissions >> createProjects).accepted

    ()
  }

  "creating the view" should {
    "create a context" in {
      val payload = jsonContentOf("kg/views/context.json")
      projects.parTraverse { project =>
        val endpoint = s"/resources/$project/resource/test-resource:context"
        deltaClient.put[Json](endpoint, payload, ScoobyDoo) { expectCreated }
      }
    }

    "create an Sparql view that index tags" in {
      val payload = jsonContentOf("kg/views/sparql-view.json")
      deltaClient.put[Json](s"/views/$project1/test-resource:cell-view", payload, ScoobyDoo) { expectCreated }
    }

    "get the created SparqlView" in {
      deltaClient.get[Json](s"/views/$project1/test-resource:cell-view", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        val viewId   = "https://dev.nexus.test.com/simplified-resource/cell-view"
        val expected = jsonContentOf(
          "kg/views/sparql-view-response.json",
          replacements(
            ScoobyDoo,
            "id"             -> "https://dev.nexus.test.com/simplified-resource/cell-view",
            "self"           -> viewSelf(project1, viewId),
            "project-parent" -> project1
          )*
        )

        filterMetadataKeys(json) should equalIgnoreArrayOrder(expected)
      }
    }

    "create an AggregateSparqlView" in {
      val payload = jsonContentOf("kg/views/agg-sparql-view.json", "project1" -> project1, "project2" -> project2)

      deltaClient.put[Json](s"/views/$project2/test-resource:agg-cell-view", payload, ScoobyDoo) { expectCreated }
    }

    "get an AggregateSparqlView" in {
      deltaClient.get[Json](s"/views/$project2/test-resource:agg-cell-view", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        val viewId   = "https://dev.nexus.test.com/simplified-resource/agg-cell-view"
        val expected = jsonContentOf(
          "kg/views/agg-sparql-view-response.json",
          replacements(
            ScoobyDoo,
            "id"             -> viewId,
            "resources"      -> viewSelf(project2, viewId),
            "project-parent" -> project2,
            "project1"       -> project1,
            "project2"       -> project2
          )*
        )

        filterMetadataKeys(json) should equalIgnoreArrayOrder(expected)
      }
    }

    "post instances" in {
      (1 to 8).toList.parTraverse { i =>
        val payload      = jsonContentOf(s"kg/views/instances/instance$i.json")
        val unprefixedId = patchedCellId(payload)
        val projectId    = if i > 5 then project2 else project1
        val indexingMode = if i % 2 == 0 then "sync" else "async"
        val targetUrl    = s"/resources/$projectId/resource/patchedcell:$unprefixedId?indexing=$indexingMode"
        deltaClient.put[Json](targetUrl, payload, ScoobyDoo) { expectCreated }
      }
    }

    "all have a completed indexing status" in eventually {
      val expected = json"""{"status": "Completed"}"""
      (1 to 5).toList.parTraverse { i =>
        val payload   = jsonContentOf(s"kg/views/instances/instance$i.json")
        val encodedId = UrlUtils.encodeUriPath(resourceId(payload))
        deltaClient.get[Json](s"/views/$project1/graph/status/$encodedId", ScoobyDoo) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          json shouldEqual expected
        }
      }
    }

    val query =
      """
        |prefix nsg: <https://bbp-nexus.epfl.ch/vocabs/bbp/neurosciencegraph/core/v0.1.0/>
        |
        |select ?s where {
        |  ?s nsg:brainLocation / nsg:brainRegion <http://www.parcellation.org/0000013>
        |}
        |order by ?s
      """.stripMargin

    "search instances in SPARQL endpoint in project 1" in eventually {
      val endpoint = s"/views/$project1/nxv:defaultSparqlIndex/sparql"
      deltaClient.sparqlQuery[Json](endpoint, query, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        json shouldEqual jsonContentOf("kg/views/sparql-search-response.json")
      }
    }

    "search instances in SPARQL endpoint in project 2" in eventually {
      val endpoint = s"/views/$project2/nxv:defaultSparqlIndex/sparql"
      deltaClient.sparqlQuery[Json](endpoint, query, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        json shouldEqual jsonContentOf("kg/views/sparql-search-response-2.json")
      }
    }

    "search instances in AggregateSparqlView when logged" in {
      val endpoint = s"/views/$project2/test-resource:agg-cell-view/sparql"
      deltaClient.sparqlQuery[Json](endpoint, query, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        json should equalIgnoreArrayOrder(jsonContentOf("kg/views/sparql-search-response-aggregated.json"))
      }
    }

    "search instances in AggregateSparqlView as anonymous" in {
      val endpoint = s"/views/$project2/test-resource:agg-cell-view/sparql"
      deltaClient.sparqlQuery[Json](endpoint, query, Anonymous) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        json should equalIgnoreArrayOrder(jsonContentOf("kg/views/sparql-search-response-2.json"))
      }
    }

    "fetch statistics for cell-view" in eventually {
      deltaClient.get[Json](s"/views/$project1/test-resource:cell-view/statistics", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        expectEmptyStats(json)
      }
    }

    "fetch statistics for defaultSparqlIndex" in eventually {
      deltaClient.get[Json](s"/views/$project1/nxv:defaultSparqlIndex/statistics", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        expectStats(json)(6, 6, 6, 0, 0, 0)
      }
    }

    "get no instances in SPARQL endpoint in project 1 with cell-view" in {
      val endpoint = s"/views/$project1/test-resource:cell-view/sparql"
      deltaClient.sparqlQuery[Json](endpoint, query, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        json shouldEqual jsonContentOf("kg/views/sparql-search-response-empty.json")
      }
    }

    "tag resources" in {
      val tagPayload = json"""{ "rev": 1, "tag": "one"}"""
      (1 to 5).toList.parTraverse { i =>
        val payload      = jsonContentOf(s"kg/views/instances/instance$i.json")
        val unprefixedId = patchedCellId(payload)
        val endpoint     = s"/resources/$project1/resource/patchedcell:$unprefixedId/tags?rev=1"
        deltaClient.post[Json](endpoint, tagPayload, ScoobyDoo) { expectCreated }
      }
    }

    "fetch updated statistics for cell-view" in eventually {
      deltaClient.get[Json](s"/views/$project1/test-resource:cell-view/statistics", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        expectStats(json)(5, 5, 5, 0, 0, 0)
      }
    }

    "search by tag in SPARQL endpoint in project 1 with default view" in eventually {
      val byTagQuery =
        """
          |prefix nxv: <https://bluebrain.github.io/nexus/vocabulary/>
          |
          |select ?s where {
          |  ?s nxv:tags "one"
          |}
          |order by ?s
        """.stripMargin
      val endpoint   = s"/views/$project1/nxv:defaultSparqlIndex/sparql"
      deltaClient.sparqlQuery[Json](endpoint, byTagQuery, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        json shouldEqual jsonContentOf("kg/views/sparql-search-response-tagged.json")
      }
    }

    "search instances in SPARQL endpoint in project 1 with custom SparqlView after tags added" in {
      val endpoint = s"/views/$project1/test-resource:cell-view/sparql"
      eventually {
        deltaClient.sparqlQuery[Json](endpoint, query, ScoobyDoo) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          json shouldEqual jsonContentOf("kg/views/sparql-search-response.json")
        }
      }
    }

    "delete tags" in {
      (1 to 5).toList.parTraverse { i =>
        val payload      = jsonContentOf(s"kg/views/instances/instance$i.json")
        val unprefixedId = patchedCellId(payload)
        val endpoint     = s"/resources/$project1/resource/patchedcell:$unprefixedId/tags/one?rev=2"
        deltaClient.delete[Json](endpoint, ScoobyDoo) { expectOk }
      }
    }

    "search instances in SPARQL endpoint in project 1 with custom SparqlView after tags are deleted" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view/sparql"
      deltaClient.sparqlQuery[Json](endpoint, query, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        json shouldEqual jsonContentOf("kg/views/sparql-search-response-empty.json")
      }
    }

    "remove @type on a resource" in {
      val payload          = filterKey("@type")(jsonContentOf("kg/views/instances/instance1.json"))
      val payloadWithoutId = filterKey("@id")(payload)
      val unprefixedId     = patchedCellId(payload)
      val endpoint         = s"/resources/$project1/_/patchedcell:$unprefixedId?rev=3"
      deltaClient.put[Json](endpoint, payloadWithoutId, ScoobyDoo) { expectOk }
    }

    "deprecate a resource" in {
      val payload      = jsonContentOf("kg/views/instances/instance2.json")
      val unprefixedId = patchedCellId(payload)
      deltaClient.delete[Json](s"/resources/$project1/_/patchedcell:$unprefixedId?rev=3", ScoobyDoo) { expectOk }
    }

    "create a another SPARQL view" in {
      val payload = jsonContentOf("kg/views/sparql-view.json")
      deltaClient.put[Json](s"/views/$project1/test-resource:cell-view2", payload, ScoobyDoo) { expectCreated }
    }

    "update a new SPARQL view" in {
      val payload = jsonContentOf("kg/views/sparql-view.json").mapObject(
        _.remove("resourceTag").remove("resourceTypes").remove("resourceSchemas")
      )
      deltaClient.put[Json](s"/views/$project1/test-resource:cell-view2?rev=1", payload, ScoobyDoo) { expectOk }
    }

    "restart the view indexing" in eventually {
      val expected =
        json"""{ "@context" : "https://bluebrain.github.io/nexus/contexts/offset.json", "@type" : "Start" }"""
      deltaClient.delete[Json](s"/views/$project1/test-resource:cell-view/offset", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        json shouldEqual expected
      }
    }

    "reindex a resource after view undeprecation" in {
      givenADeprecatedView { view =>
        val resource = genId()
        postResource(resource, indexing = "sync") >>
          undeprecate(view) >>
          assertResourceIndexed(view, resource)
      }
    }

    def givenAView(test: String => IO[Assertion]): IO[Assertion] = {
      val viewId      = genId()
      val viewPayload = jsonContentOf("kg/views/sparql-view-index-all.json", "withTag" -> false)
      val createView  = deltaClient.put[Json](s"/views/$project1/$viewId", viewPayload, ScoobyDoo) { expectCreated }

      createView >> test(viewId)
    }

    def givenADeprecatedView(test: String => IO[Assertion]): IO[Assertion] =
      givenAView { view =>
        val deprecateView = deltaClient.delete[Json](s"/views/$project1/$view?rev=1", ScoobyDoo) { expectOk }
        deprecateView >> test(view)
      }

    def undeprecate(view: String, rev: Int = 2): IO[Assertion] =
      deltaClient.putEmptyBody[Json](s"/views/$project1/$view/undeprecate?rev=$rev", ScoobyDoo) { expectOk }
  }

  "Resuming view indexing after passivation" should {

    "index a new resource in a custom and the default Sparql view once they have passivated and been evicted" in {
      val viewId      = genId()
      val resourceId  = genId()
      val viewPayload = jsonContentOf("kg/views/sparql-view-index-all.json", "withTag" -> false)
      for {
        _   <- deltaClient.put[Json](s"/views/$project1/$viewId", viewPayload, ScoobyDoo)(expectCreated)
        _   <- waitUntilProjectionRunning(viewId)
        _   <- waitUntilProjectionRunning("defaultSparqlIndex")
        _   <- waitUntilProjectionEvicted(viewId)
        _   <- waitUntilProjectionEvicted("defaultSparqlIndex")
        // A single new resource bumps the project's activity, which must resume both projections so they index again.
        _   <- postResource(resourceId)
        _   <- assertResourceIndexed(viewId, resourceId)
        res <- assertResourceIndexed("nxv:defaultSparqlIndex", resourceId)
      } yield res
    }

    // Projection names are `blazegraph-$project-$viewIri-$rev`, so matching on project1 + the fragment pins the check to
    // this project's view (and excludes the never-passivating default composite view).
    def waitUntilProjectionRunning(fragment: String): IO[Assertion] =
      eventually(adminDsl.assertProjectionRunning(project1, fragment))

    def waitUntilProjectionEvicted(fragment: String): IO[Assertion] =
      eventually(adminDsl.assertProjectionEvicted(project1, fragment))
  }

  /** Creates a minimal resource in project1 with the given id, using the given indexing mode (async by default). */
  private def postResource(id: String, indexing: String = "async"): IO[Assertion] =
    deltaClient.post[Json](s"/resources/$project1/_?indexing=$indexing", json"""{"@id": "$idBase$id"}""", ScoobyDoo)(
      expectCreated
    )

  /** Polls the given view's Sparql endpoint until the resource with the given id has been indexed. */
  private def assertResourceIndexed(view: String, id: String): IO[Assertion] = {
    val query = s"""SELECT (COUNT(*) as ?count) WHERE { <$idBase$id> ?p ?o }""".stripMargin
    eventually {
      deltaClient.sparqlQuery[Json](s"/views/$project1/$view/sparql", query, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        sparql.countResult(json).value should be > 0
      }
    }
  }
}
