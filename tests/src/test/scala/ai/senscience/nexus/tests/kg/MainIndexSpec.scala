package ai.senscience.nexus.tests.kg

import ai.senscience.nexus.delta.kernel.utils.UrlUtils.encodeUriPath
import ai.senscience.nexus.tests.BaseIntegrationSpec
import ai.senscience.nexus.tests.Identity.listings.{Alice, Bob}
import ai.senscience.nexus.tests.Optics.listing._results
import ai.senscience.nexus.tests.Optics.{_total, hitProjects}
import ai.senscience.nexus.tests.StatisticsAssertions.expectStats
import ai.senscience.nexus.tests.admin.ProjectPayload
import ai.senscience.nexus.tests.iam.types.Permission.Organizations
import ai.senscience.nexus.tests.resources.SimpleResource
import cats.syntax.all.*
import io.circe.Json
import org.apache.pekko.http.scaladsl.model.StatusCodes

class MainIndexSpec extends BaseIntegrationSpec {

  private val org1   = genId()
  private val proj11 = genId()
  private val proj12 = genId()
  private val ref11  = s"$org1/$proj11"
  private val ref12  = s"$org1/$proj12"

  override def beforeAll(): Unit = {
    super.beforeAll()

    val setup = for {
      _               <- aclDsl.addPermission("/", Bob, Organizations.Create)
      // First org and projects
      _               <- adminDsl.createOrganization(org1, org1, Bob)
      _               <- adminDsl.createProject(org1, proj11, ProjectPayload.generate(proj11), Bob)
      _               <- adminDsl.createProject(org1, proj12, ProjectPayload.generate(proj12), Bob)
      resourcePayload <- SimpleResource.sourcePayload(5)
      resources        = List(ref11 -> "r11_1", ref11 -> "r11_2", ref12 -> "r12_1", ref12 -> "r12_2")
      _               <- resources.parTraverse { case (proj, id) =>
                           deltaClient.put[Json](s"/resources/$proj/_/$id", resourcePayload, Bob)(expectCreated)
                         }
    } yield ()
    setup.accepted
  }

  private val defaultViewsId = encodeUriPath("https://bluebrain.github.io/nexus/vocabulary/defaultElasticSearchIndex")

  "Getting main indexing statistics" should {

    "get an error if the user has no access" in {
      deltaClient.get[Json](s"/views/$ref11/$defaultViewsId/statistics", Alice) { expectForbidden }
    }

    "get the statistics if the user has access" in eventually {
      deltaClient.get[Json](s"/views/$ref11/$defaultViewsId/statistics", Bob) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        expectStats(json)(2, 2, 2, 0, 0, 0)
      }
    }
  }

  "Getting main offset" should {

    "get an error if the user has no access" in {
      deltaClient.get[Json](s"/views/$ref11/$defaultViewsId/offset", Alice) { expectForbidden }
    }

    "get the statistics if the user has access" in eventually {
      deltaClient.get[Json](s"/views/$ref11/$defaultViewsId/offset", Bob) { expectOk }
    }
  }

  "Getting indexing failures" should {
    "get an error if the user has no access" in {
      deltaClient.get[Json](s"/views/$ref11/$defaultViewsId/failures", Alice) { expectForbidden }
    }

    "get no failure" in eventually {
      deltaClient.get[Json](s"/views/$ref11/$defaultViewsId/failures", Bob) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        _total.getOption(json).value shouldEqual 0L
        _results.getOption(json).value should be(empty)
      }
    }
  }

  "Deleting main offset" should {

    "get an error if the user has no access" in {
      deltaClient.delete[Json](s"/views/$ref11/$defaultViewsId/offset", Alice) { expectForbidden }
    }

    "get the statistics if the user has access" in eventually {
      deltaClient.delete[Json](s"/views/$ref11/$defaultViewsId/offset", Bob) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        val expected =
          json"""{ "@context" : "https://bluebrain.github.io/nexus/contexts/offset.json", "@type" : "Start" }"""
        json shouldEqual expected
      }
    }
  }

  "Searching on the main index" should {

    val matchAll = json"""{"query": { "match_all": {} } }"""

    "get an error for a user with no access" in {
      deltaClient.post[Json](s"/views/$ref11/$defaultViewsId/_search", matchAll, Alice) { expectForbidden }
    }

    s"get a response with only resources from project '$ref11'" in eventually {
      deltaClient.post[Json](s"/views/$ref11/$defaultViewsId/_search", matchAll, Bob) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        hitProjects.getAll(json) should contain only ref11
      }
    }
  }
}
