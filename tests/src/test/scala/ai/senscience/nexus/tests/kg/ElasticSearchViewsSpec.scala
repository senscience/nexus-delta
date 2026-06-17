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
import io.circe.{ACursor, Json}
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.scalatest.Assertion

class ElasticSearchViewsSpec extends BaseIntegrationSpec {

  private val orgId  = genId()
  private val projId = genId()
  val project1       = s"$orgId/$projId"

  private val projId2 = genId()
  val project2        = s"$orgId/$projId2"

  private val projects = List(project1, project2)

  /** Base IRI shared by the patched-cell instance fixtures. */
  private val patchedCellBase = "https://bbp.epfl.ch/nexus/v0/data/bbp/experiment/patchedcell/v0.1.0/"

  /** The `@id` declared in a resource payload. */
  private def resourceId(payload: Json): String = `@id`.getOption(payload).value

  /** The local part of a patched-cell resource `@id`, used to build `patchedcell:<suffix>` paths. */
  private def patchedCellId(payload: Json): String = resourceId(payload).stripPrefix(patchedCellBase)

  /**
    * Strips the non-deterministic parts from an Elasticsearch `_search` response so it can be compared to a fixture:
    * the `took` timing and each hit's `_index` (whose name embeds a uuid and revision).
    */
  private val filterSearchMetadata: Json => Json = filterNestedKeys("took", "_index")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val setup = for {
      _ <- aclDsl.addPermission("/", ScoobyDoo, Organizations.Create)
      _ <- aclDsl.addPermissionAnonymous(s"/$project2", Views.Query)
      _ <- adminDsl.createOrganization(orgId, orgId, ScoobyDoo)
      _ <- adminDsl.createProjectWithName(orgId, projId, name = project1, ScoobyDoo)
      _ <- adminDsl.createProjectWithName(orgId, projId2, name = project2, ScoobyDoo)
    } yield succeed

    setup.accepted
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

    "create elasticsearch views with legacy fields and its pipeline equivalent" in {
      List(project1 -> "kg/views/elasticsearch/legacy-fields.json", project2 -> "kg/views/elasticsearch/pipeline.json")
        .parTraverse { case (project, file) =>
          val endpoint = s"/views/$project/test-resource:cell-view"
          val payload  = jsonContentOf(file, "withTag" -> false)
          deltaClient.put[Json](endpoint, payload, ScoobyDoo) { expectCreated }
        }
    }

    "create elasticsearch views filtering on tag with legacy fields and its pipeline equivalent" in {
      List(project1 -> "kg/views/elasticsearch/legacy-fields.json", project2 -> "kg/views/elasticsearch/pipeline.json")
        .parTraverse { case (project, file) =>
          val endpoint = s"/views/$project/test-resource:cell-view-tagged"
          val payload  = jsonContentOf(file, "withTag" -> true)
          deltaClient.put[Json](endpoint, payload, ScoobyDoo) { expectCreated }
        }
    }

    "fail to create a view with an invalid mapping" in {
      val invalidMapping = json"""{"mapping": "fail"}"""
      val invalidPayload = json"""{ "@type": "ElasticSearchView", "mapping": $invalidMapping }"""
      deltaClient.put[Json](s"/views/$project1/invalid", invalidPayload, ScoobyDoo) { expectBadRequest }
    }

    "fail to create a view with invalid settings" in {
      val invalidSettings = json"""{"analysis": "fail"}"""
      val invalidPayload  = json"""{ "@type": "ElasticSearchView", "mapping": { }, "settings": $invalidSettings }"""
      deltaClient.put[Json](s"/views/$project1/invalid", invalidPayload, ScoobyDoo) { expectBadRequest }
    }

    "create people view in project 2" in {
      val payload = jsonContentOf("kg/views/elasticsearch/people-view.json")
      deltaClient.put[Json](s"/views/$project2/test-resource:people", payload, ScoobyDoo) { expectCreated }
    }

    "get the created elasticsearch views" in {
      val id = "https://dev.nexus.test.com/simplified-resource/cell-view"
      projects.parTraverse { project =>
        deltaClient.get[Json](s"/views/$project/test-resource:cell-view", ScoobyDoo) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          val expected = jsonContentOf(
            "kg/views/elasticsearch/indexing-response.json",
            replacements(
              ScoobyDoo,
              "id"      -> id,
              "self"    -> viewSelf(project, id),
              "project" -> project
            )*
          )
          filterMetadataKeys(json) should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "create an AggregateElasticSearchView" in {
      elasticsearchViewsDsl.aggregate(
        "test-resource:agg-cell-view",
        project2,
        ScoobyDoo,
        project1 -> "https://dev.nexus.test.com/simplified-resource/cell-view",
        project2 -> "https://dev.nexus.test.com/simplified-resource/cell-view"
      )
    }

    "get the created AggregateElasticSearchView" in {
      val id = "https://dev.nexus.test.com/simplified-resource/agg-cell-view"
      deltaClient.get[Json](s"/views/$project2/test-resource:agg-cell-view", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK

        val expected = jsonContentOf(
          "kg/views/elasticsearch/aggregate-response.json",
          replacements(
            ScoobyDoo,
            "id"       -> id,
            "self"     -> viewSelf(project2, id),
            "project1" -> project1,
            "project2" -> project2
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
        val endpoint     = s"/resources/$projectId/resource/patchedcell:$unprefixedId?indexing=$indexingMode"
        deltaClient.put[Json](endpoint, payload, ScoobyDoo) { expectCreated }
      }
    }

    "post instance without id" in {
      val payload = jsonContentOf("kg/views/instances/instance9.json")
      deltaClient.post[Json](s"/resources/$project2/resource?indexing=sync", payload, ScoobyDoo) { expectCreated }
    }

    "wait until all instances are indexed in default view of project 2" in eventually {
      deltaClient.get[Json](s"/resources/$project2/resource", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        _total.getOption(json).value shouldEqual 5
      }
    }

    "all have a completed indexing status" in eventually {
      val expected = json"""{"status": "Completed"}"""
      (1 to 5).toList.parTraverse { i =>
        val payload   = jsonContentOf(s"kg/views/instances/instance$i.json")
        val encodedId = UrlUtils.encodeUriPath(resourceId(payload))
        val endpoint  = s"/views/$project1/test-resource:cell-view/status/$encodedId"
        deltaClient.get[Json](endpoint, ScoobyDoo) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          json shouldEqual expected
        }
      }
    }

    val invalidElasticQuery = json"""{ "query": { "other": {} } }"""

    "return 400 with bad query instances on main and custom views" in {
      List("test-resource:cell-view", "documents").traverse { view =>
        deltaClient.post[Json](s"/views/$project1/$view/_search", invalidElasticQuery, ScoobyDoo) { expectBadRequest }
      }
    }

    val sort             = json"""{ "sort": [{ "name.raw": { "order": "asc" } }] }"""
    val sortedMatchCells = json"""{ "query": { "term": { "@type": "Cell" } } }""".deepMerge(sort)
    val matchAll         = json"""{ "query": { "match_all": {} } }""".deepMerge(sort)

    "search instances on project 1 in cell-view" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view/_search"
      for {
        sorted <- deltaClient.postAndReturn[Json](endpoint, sortedMatchCells, ScoobyDoo) { (json, response) =>
                    response.status shouldEqual StatusCodes.OK
                    filterSearchMetadata(json) shouldEqual jsonContentOf("kg/views/elasticsearch/search-response.json")
                  }
        all    <- deltaClient.postAndReturn[Json](endpoint, matchAll, ScoobyDoo) { (_, response) =>
                    response.status shouldEqual StatusCodes.OK
                  }
      } yield filterSearchMetadata(all) shouldEqual filterSearchMetadata(sorted)
    }

    "get no instance in cell-view-tagged in project1 as nothing is tagged yet" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view-tagged/_search"
      deltaClient.post[Json](endpoint, matchAll, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        totalHits.getOption(json).value shouldEqual 0
      }
    }

    "search cell instances on project 2" in eventually {
      val endpoint = s"/views/$project2/test-resource:cell-view/_search"
      for {
        sorted <- deltaClient.postAndReturn[Json](endpoint, sortedMatchCells, ScoobyDoo) { (json, response) =>
                    response.status shouldEqual StatusCodes.OK
                    filterSearchMetadata(json) shouldEqual jsonContentOf(
                      "kg/views/elasticsearch/search-response-2.json"
                    )
                  }
        all    <- deltaClient.postAndReturn[Json](endpoint, matchAll, ScoobyDoo) { (_, response) =>
                    response.status shouldEqual StatusCodes.OK
                  }
      } yield filterSearchMetadata(all) shouldEqual filterSearchMetadata(sorted)
    }

    "the person resource created with no id in payload should have the default id in _source" in eventually {
      val endpoint = s"/views/$project2/test-resource:people/_search"
      deltaClient.post[Json](endpoint, matchAll, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        val id       = hits(0)._id.string.getOption(json)
        val sourceId = hits(0)._source.`@id`.string.getOption(json)
        sourceId shouldEqual id
      }
    }

    "get no instance is indexed in cell-view-tagged in project2 as nothing is tagged yet" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view-tagged/_search"
      deltaClient.post[Json](endpoint, matchAll, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        totalHits.getOption(json).value shouldEqual 0
      }
    }

    "search instances on project AggregatedElasticSearchView when logged" in eventually {
      val endpoint = s"/views/$project2/test-resource:agg-cell-view/_search"
      deltaClient.post[Json](endpoint, sortedMatchCells, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        filterSearchMetadata(json) shouldEqual jsonContentOf("kg/views/elasticsearch/search-response-aggregated.json")
      }
    }

    "search instances on project AggregatedElasticSearchView as anonymous" in eventually {
      val endpoint = s"/views/$project2/test-resource:agg-cell-view/_search"
      deltaClient.post[Json](endpoint, sortedMatchCells, Anonymous) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        filterSearchMetadata(json) shouldEqual jsonContentOf("kg/views/elasticsearch/search-response-2.json")
      }
    }

    "fetch statistics for cell-view" in eventually {
      deltaClient.get[Json](s"/views/$project1/test-resource:cell-view/statistics", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        expectStats(json)(5, 5, 5, 0, 0, 0)
      }
    }

    "fetch statistics for cell-view-tagged" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view-tagged/statistics"
      deltaClient.get[Json](endpoint, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        expectEmptyStats(json)
      }
    }

    "tag resources" in {
      (1 to 5).toList.parTraverse { i =>
        val payload      = jsonContentOf(s"kg/views/instances/instance$i.json")
        val unprefixedId = patchedCellId(payload)
        val endpoint     = s"/resources/$project1/resource/patchedcell:$unprefixedId/tags?rev=1"
        val tagPayload   = Json.obj("rev" -> Json.fromInt(1), "tag" -> Json.fromString("one"))
        deltaClient.post[Json](endpoint, tagPayload, ScoobyDoo) { expectCreated }
      }
    }

    "get newly tagged instances in cell-view-tagged in project1" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view-tagged/_search"
      deltaClient.post[Json](endpoint, matchAll, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        val total = totalHits.getOption(json).value
        total shouldEqual 5
      }
    }

    "get updated statistics for cell-view-tagged" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view-tagged/statistics"
      deltaClient.get[Json](endpoint, ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        expectStats(json)(5, 5, 5, 0, 0, 0)
      }
    }

    "remove @type on a resource" in {
      val payload          = filterKey("@type")(jsonContentOf("kg/views/instances/instance1.json"))
      val payloadWithoutId = filterKey("@id")(payload)
      val unprefixedId     = patchedCellId(payload)
      val endpoint         = s"/resources/$project1/_/patchedcell:$unprefixedId?rev=2"
      deltaClient.put[Json](endpoint, payloadWithoutId, ScoobyDoo) { expectOk }
    }

    "search instances on project 1 after removed @type" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view/_search"
      for {
        sorted <- deltaClient.postAndReturn[Json](endpoint, sortedMatchCells, ScoobyDoo) { (json, response) =>
                    response.status shouldEqual StatusCodes.OK
                    filterSearchMetadata(json) shouldEqual jsonContentOf(
                      "kg/views/elasticsearch/search-response-no-type.json"
                    )
                  }
        all    <- deltaClient.postAndReturn[Json](endpoint, matchAll, ScoobyDoo) { (_, response) =>
                    response.status shouldEqual StatusCodes.OK
                  }
      } yield filterSearchMetadata(all) shouldEqual filterSearchMetadata(sorted)
    }

    "deprecate a resource" in {
      val payload      = filterKey("@type")(jsonContentOf("kg/views/instances/instance2.json"))
      val unprefixedId = patchedCellId(payload)
      deltaClient.delete[Json](s"/resources/$project1/_/patchedcell:$unprefixedId?rev=2", ScoobyDoo) { expectOk }
    }

    "search instances on project 1 after deprecated" in eventually {
      val endpoint = s"/views/$project1/test-resource:cell-view/_search"
      for {
        sorted <- deltaClient.postAndReturn[Json](endpoint, sortedMatchCells, ScoobyDoo) { (json, response) =>
                    response.status shouldEqual StatusCodes.OK
                    filterSearchMetadata(json) shouldEqual jsonContentOf(
                      "kg/views/elasticsearch/search-response-no-deprecated.json"
                    )
                  }
        all    <- deltaClient.postAndReturn[Json](endpoint, matchAll, ScoobyDoo) { (_, response) =>
                    response.status shouldEqual StatusCodes.OK
                  }
      } yield filterSearchMetadata(all) shouldEqual filterSearchMetadata(sorted)
    }

    "restart the view indexing" in eventually {
      deltaClient.delete[Json](s"/views/$project1/test-resource:cell-view/offset", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        val expected =
          json"""{ "@context" : "https://bluebrain.github.io/nexus/contexts/offset.json", "@type" : "Start" }"""
        json shouldEqual expected
      }
    }

    "fail to fetch mapping without permission" in {
      deltaClient.get[Json](s"/views/$project1/test-resource:cell-view/_mapping", Anonymous) { expectForbidden }
    }

    "fail to fetch mapping for view that doesn't exist" in {
      deltaClient.get[Json](s"/views/$project1/test-resource:wrong-view/_mapping", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.NotFound
        json should have(`@type`("ResourceNotFound"))
      }
    }

    "fail to fetch mapping for aggregate view" in {
      val view = "test-resource:agg-cell-view"
      deltaClient.get[Json](s"/views/$project2/$view/_mapping", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.BadRequest
        json should have(`@type`("DifferentElasticSearchViewType"))
      }
    }

    "return the view's mapping" in {
      deltaClient.get[Json](s"/views/$project1/test-resource:cell-view/_mapping", ScoobyDoo) { (json, response) =>
        response.status shouldEqual StatusCodes.OK

        def hasOnlyOneKey = (j: ACursor) => j.keys.exists(_.size == 1)
        def downFirstKey  = (j: ACursor) => j.downField(j.keys.get.head)

        assert(hasOnlyOneKey(json.hcursor))
        val firstKey = downFirstKey(json.hcursor)
        assert(hasOnlyOneKey(firstKey))
        assert(downFirstKey(firstKey).key.contains("mappings"))
      }
    }

    "undeprecate a deprecated view" in {
      givenADeprecatedView { view =>
        val assertUndeprecated = deltaClient.get[Json](s"/views/$project1/$view", ScoobyDoo) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          json.hcursor.get[Boolean]("_deprecated").toOption should contain(false)
        }
        undeprecate(view) >> assertUndeprecated
      }
    }

    "reindex a resource after a view is undeprecated" in {
      givenADeprecatedView { view =>
        givenAPersonResource { person =>
          undeprecate(view) >> assertMatchId(view, person)
        }
      }
    }
  }

  "Resuming view indexing after passivation" should {

    "index a new resource in a custom Elasticsearch view once it has passivated and been evicted" in {
      val payload = jsonContentOf("kg/resources/person.json")
      givenAView { view =>
        val personId = genId()
        for {
          _   <- waitUntilProjectionRunning(view)
          _   <- waitUntilProjectionEvicted(view)
          _   <- deltaClient.put[Json](s"/resources/$project1/_/$personId", payload, ScoobyDoo)(expectCreated)
          res <- assertMatchId(view, personId)
        } yield res
      }
    }
  }

  def givenAView(test: String => IO[Assertion]): IO[Assertion] = {
    val viewId      = genId()
    val viewPayload = jsonContentOf("kg/views/elasticsearch/people-view.json", "withTag" -> false)
    val createView  = deltaClient.put[Json](s"/views/$project1/$viewId", viewPayload, ScoobyDoo) { expectCreated }

    createView >> test(viewId)
  }

  def givenADeprecatedView(test: String => IO[Assertion]): IO[Assertion] =
    givenAView { view =>
      val deprecateView = deltaClient.delete[Json](s"/views/$project1/$view?rev=1", ScoobyDoo) { expectOk }
      deprecateView >> test(view)
    }

  def givenAPersonResource(test: String => IO[Assertion]): IO[Assertion] = {
    val id = genId()
    deltaClient.put[Json](
      s"/resources/$project1/_/$id?indexing=sync",
      jsonContentOf("kg/resources/person.json"),
      ScoobyDoo
    ) { expectCreated } >> test(id)
  }

  def undeprecate(view: String, rev: Int = 2): IO[Assertion] =
    deltaClient.putEmptyBody[Json](s"/views/$project1/$view/undeprecate?rev=$rev", ScoobyDoo) { expectOk }

  def assertMatchId(view: String, id: String): IO[Assertion] =
    eventually {
      deltaClient
        .post[Json](
          s"/views/$project1/$view/_search",
          json"""{ "query": { "match": { "@id": "$id" } } }""",
          ScoobyDoo
        ) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          totalHits.getOption(json).value shouldEqual 1
        }
    }

  // Projection names are `elasticsearch-$project-$viewIri-$rev`, so matching on project1 + the fragment pins the check
  // to this project's custom view.
  def waitUntilProjectionRunning(fragment: String): IO[Assertion] =
    eventually(adminDsl.assertProjectionRunning(project1, fragment))

  def waitUntilProjectionEvicted(fragment: String): IO[Assertion] =
    eventually(adminDsl.assertProjectionEvicted(project1, fragment))
}
