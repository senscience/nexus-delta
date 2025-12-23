package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.model.permissions as esPermissions
import ai.senscience.nexus.delta.elasticsearch.query.ConfiguredIndexQuery
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect
import cats.effect.IO
import cats.effect.kernel.Ref
import io.circe.{Json, JsonObject}
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route
import org.http4s.Query

class ConfiguredIndexRoutesSpec extends ElasticSearchViewsRoutesFixtures {

  private val project1 = ProjectRef.unsafe("org", "proj1")
  private val project2 = ProjectRef.unsafe("org", "proj2")

  private val searchResult = json"""{ "success":  true }"""

  private val refRefresh = Ref.unsafe[IO, Boolean](false)

  private val configuredQuery = new ConfiguredIndexQuery {
    override def search(
        project: ProjectRef,
        target: ConfiguredIndexQuery.Target,
        query: JsonObject,
        qp: Query
    ): effect.IO[Json] =
      IO.pure(searchResult)

    override def refresh: effect.IO[Unit] = refRefresh.set(true)
  }

  private lazy val routes =
    Route.seal(
      new ConfiguredIndexRoutes(
        identities,
        aclCheck,
        configuredQuery,
        ProjectionsDirectives.testEcho
      ).routes
    )

  override def afterAll(): Unit = {}

  override def beforeAll(): Unit = {
    super.beforeAll()
    val setup = aclCheck.append(AclAddress.Project(project1), reader -> Set(esPermissions.query, esPermissions.read)) >>
      aclCheck.append(AclAddress.Project(project1), writer -> Set(esPermissions.write)) >>
      aclCheck.append(AclAddress.Root, admin -> Set(esPermissions.write))

    setup.accepted
  }

  "Main index route" should {
    s"fail to get statistics if the user has no access to $project2" in {
      Get(s"/index/configured/$project2/_/statistics") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    s"get statistics if the user has access to $project1" in {
      Get(s"/index/configured/$project1/_/statistics") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asString shouldEqual "indexing-statistics"
      }
    }

    s"fail to get offset if the user has no access to $project2" in {
      Get(s"/index/configured/$project2/_/offset") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    s"get offset if the user has access to $project1" in {

      Get(s"/index/configured/$project1/_/offset") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asString shouldEqual "offset"
      }
    }

    s"fail to delete offset if the user has no write access to $project1" in {
      Delete(s"/index/configured/$project1/_/offset") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    s"get offset if the user has write access to $project1" in {

      Delete(s"/index/configured/$project1/_/offset") ~> as(writer) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asString shouldEqual "schedule-restart"
      }
    }

    s"fail perform a search if the user has no access to $project2" in {
      Post(s"/index/configured/$project2/_/_search", json"""{}""".toEntity) ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    s"return a search if the user has no access to $project1" in {
      Post(s"/index/configured/$project1/_/_search", json"""{}""".toEntity) ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asJson shouldEqual searchResult
      }
    }
  }
}
