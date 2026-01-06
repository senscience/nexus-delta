package ai.senscience.nexus.tests.kg

import ai.senscience.nexus.delta.kernel.utils.UrlUtils.encodeUriPath
import ai.senscience.nexus.testkit.CirceLiteral.circeLiteralSyntax
import ai.senscience.nexus.tests.BaseIntegrationSpec
import ai.senscience.nexus.tests.Identity.listings.{Alice, Bob}
import ai.senscience.nexus.tests.Optics.listing._results
import ai.senscience.nexus.tests.Optics.{_total, hitProjects, hitsSource, totalHits}
import ai.senscience.nexus.tests.StatisticsAssertions.expectStats
import ai.senscience.nexus.tests.admin.ProjectPayload
import ai.senscience.nexus.tests.iam.types.Permission.Organizations
import ai.senscience.nexus.tests.kg.ConfiguredIndexSpec.{creativeWork, other, person, role}
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import io.circe.{Json, JsonObject}
import org.apache.pekko.http.scaladsl.model.StatusCodes

import java.time.Instant

class ConfiguredIndexSpec extends BaseIntegrationSpec {

  private val org1   = genId()
  private val proj11 = genId()
  private val proj12 = genId()
  private val ref11  = s"$org1/$proj11"
  private val ref12  = s"$org1/$proj12"

  private val bobId = "https://senscience.ai/person/bob"

  override def beforeAll(): Unit = {
    super.beforeAll()

    val setup = for {
      _             <- aclDsl.addPermission("/", Bob, Organizations.Create)
      // First org and projects
      _             <- adminDsl.createOrganization(org1, org1, Bob)
      _             <- adminDsl.createProject(org1, proj11, ProjectPayload.generate(proj11), Bob)
      _             <- adminDsl.createProject(org1, proj12, ProjectPayload.generate(proj12), Bob)
      // Index in first project
      bobPayload     = person(bobId, "Bob", 42)
      _             <- deltaClient.post[Json](s"/resources/$ref11/_/", bobPayload, Bob)(expectCreated)
      aliceId        = "https://senscience.ai/person/alice"
      alicePayload   = person(aliceId, "Alice", 56)
      _             <- deltaClient.post[Json](s"/resources/$ref11/_/", alicePayload, Bob)(expectCreated)
      roleId         = "https://senscience.ai/role/publisher"
      rolePayload    = role(roleId, "Publisher", Instant.EPOCH)
      _             <- deltaClient.post[Json](s"/resources/$ref12/_/", rolePayload, Bob)(expectCreated)
      workId         = "https://senscience.ai/work/marine-biology"
      workPayload    = creativeWork(workId, "Marine biology", Instant.EPOCH)
      _             <- deltaClient.post[Json](s"/resources/$ref12/_/", workPayload, Bob)(expectCreated)
      otherId        = "https://senscience.ai/other/xxx"
      otherPayload   = other(otherId)
      _             <- deltaClient.post[Json](s"/resources/$ref11/_/", otherPayload, Bob)(expectCreated)
      _             <- deltaClient.post[Json](s"/resources/$ref12/_/", otherPayload, Bob)(expectCreated)
      invalidId      = "https://senscience.ai/person/xxx"
      invalidPayload = json"""{ "@type": "https://schema.org/Person", "@id": "$invalidId", "age": "XXX" }"""
      _             <- deltaClient.post[Json](s"/resources/$ref11/_/", invalidPayload, Bob)(expectCreated)
      _             <- deltaClient.post[Json](s"/resources/$ref12/_/", invalidPayload, Bob)(expectCreated)
    } yield ()
    setup.accepted
  }

  "Configured indices" should {

    "be created" in {
      elasticsearchDsl.includes("delta_person", "delta_role", "delta_creative-work")
    }
  }

  "Getting default indexing statistics" should {

    "get an error if the user has no access" in {
      deltaClient.get[Json](s"/index/configured/$ref11/_/statistics", Alice) {
        expectForbidden
      }
    }

    "get the statistics if the user has access" in eventually {
      List(ref11, ref12).traverse { ref =>
        deltaClient.get[Json](s"/index/configured/$ref/_/statistics", Bob) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          expectStats(json)(4, 4, 2, 1, 1, 0)
        }
      }
    }
  }

  "Getting configured offset" should {

    "get an error if the user has no access" in {
      deltaClient.get[Json](s"/index/configured/$ref11/_/offset", Alice) {
        expectForbidden
      }
    }

    "get the statistics if the user has access" in {
      deltaClient.get[Json](s"/index/configured/$ref11/_/offset", Bob) {
        expectOk
      }
    }
  }

  "Getting indexing failures" should {
    "get an error if the user has no access" in {
      deltaClient.get[Json](s"/index/configured/$ref11/_/failures", Alice) {
        expectForbidden
      }
    }

    "get a failure" in eventually {
      deltaClient.get[Json](s"/index/configured/$ref11/_/failures", Bob) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        _total.getOption(json).value shouldEqual 1L
        _results.getOption(json).value should have size 1
      }
    }
  }

  "Searching on the configured indices" should {

    val matchAll = json"""{"query": { "match_all": {} } }"""

    "get an error for a user with no access" in {
      deltaClient.post[Json](s"/index/configured/$ref11/_/_search", matchAll, Alice) {
        expectForbidden
      }
    }

    s"get a response with only resources from project '$ref11'" in eventually {
      deltaClient.post[Json](s"/index/configured/$ref11/_/_search", matchAll, Bob) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        totalHits.getOption(json).value shouldEqual 2
        hitProjects.getAll(json) should contain only ref11
      }
    }

    s"get a response with only roles from project '$ref12'" in eventually {
      deltaClient.post[Json](s"/index/configured/$ref12/role/_search", matchAll, Bob) { (json, response) =>
        response.status shouldEqual StatusCodes.OK
        totalHits.getOption(json).value shouldEqual 1
        hitProjects.getAll(json) should contain only ref12
      }
    }
  }

  "Updating Bob and refresh the index to have a consistent response" should {
    val encodedId = encodeUriPath(bobId)

    val queryById = json"""{ "query": { "term": { "@id" :  "$bobId"} }}"""

    def waitForIndexingAndRefresh: IO[Unit] =
      Stream
        .unfoldLoopEval("Pending") { _ =>
          deltaClient
            .getJson[JsonObject](s"/index/configured/$ref11/_/status/$encodedId", Bob)
            .map { json =>
              json -> json("status").flatMap(_.asString.filterNot(_ == "Completed"))
            }
        }
        .compile
        .drain

    "get an updated response after updating and refreshing" in {
      val updatePayload = person(bobId, "Bob", 43)

      deltaClient.put[Json](s"/resources/$ref11/_/$encodedId?rev=1", updatePayload, Bob) { expectOk } >>
        waitForIndexingAndRefresh >>
        deltaClient.post[Json](s"/index/configured/$ref11/person/_search", queryById, Bob) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          totalHits.getOption(json).value shouldEqual 1
          val hit = hitsSource.getAll(json).headOption.flatMap(_.asObject).value
          hit("@id").flatMap(_.asString).value shouldEqual bobId
          hit("_rev").flatMap(_.asNumber.flatMap(_.toInt)).value shouldEqual 2
        }

    }

    s"remove $bobId if it gets to an non-configured type" in {
      val updatePayload = other(bobId)

      deltaClient.put[Json](s"/resources/$ref11/_/$encodedId?rev=2", updatePayload, Bob) { expectOk } >>
        waitForIndexingAndRefresh >>
        deltaClient.post[Json](s"/index/configured/$ref11/_/_search", queryById, Bob) { (json, response) =>
          response.status shouldEqual StatusCodes.OK
          totalHits.getOption(json).value shouldEqual 0
        }
    }
  }
}

object ConfiguredIndexSpec {

  private def creativeWork(id: String, about: String, datePublished: Instant) =
    json"""{ "@type": "https://schema.org/CreativeWork", "@id": "$id", "about": "$about", "datePublished": "$datePublished" }"""

  private def person(id: String, name: String, age: Int) =
    json"""{ "@type": "https://schema.org/Person", "@id": "$id", "name": "$name", "age": $age }"""

  private def role(id: String, name: String, startDate: Instant) =
    json"""{ "@type": "https://schema.org/Role", "@id": "$id", "roleName": "$name", "startDate": "$startDate" }"""

  private def other(id: String) =
    json"""{ "@type": "https://schema.org/Other", "@id": "$id" }"""

}
