package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.model.{defaultViewId, permissions as esPermissions}
import ai.senscience.nexus.delta.elasticsearch.query.{MainIndexQuery, MainIndexRequest}
import ai.senscience.nexus.delta.kernel.utils.UrlUtils.encodeUriPath
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.model.search.{AggregationResult, SearchResults}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.effect.IO
import io.circe.{Json, JsonObject}
import org.http4s.Query

class MainIndexRoutesSpec extends ElasticSearchViewsRoutesFixtures {

  private val project1 = ProjectRef.unsafe("org", "proj1")
  private val project2 = ProjectRef.unsafe("org", "proj2")

  private val searchResult = json"""{ "success":  true }"""

  private val encodedDefaultViewId = encodeUriPath(defaultViewId.toString)

  private val mainIndexQuery = new MainIndexQuery {
    override def search(project: ProjectRef, query: JsonObject, qp: Query): IO[Json] =
      IO.pure(searchResult)

    override def list(request: MainIndexRequest, projects: Set[ProjectRef]): IO[SearchResults[JsonObject]] = ???

    override def aggregate(request: MainIndexRequest, projects: Set[ProjectRef]): IO[AggregationResult] = ???
  }

  private lazy val routes       =
    Route.seal(
      new MainIndexRoutes(
        identities,
        aclCheck,
        mainIndexQuery,
        ProjectionsDirectives.testEcho
      ).routes
    )
  override def afterAll(): Unit = {}

  override def beforeAll(): Unit = {
    super.beforeAll()
    val setup = for {
      _ <- aclCheck.append(AclAddress.Project(project1), reader -> Set(esPermissions.query, esPermissions.read))
      _ <- aclCheck.append(AclAddress.Project(project1), writer -> Set(esPermissions.write))
    } yield ()

    setup.accepted
  }

  "Default index route" should {
    s"fail to get statistics if the user has no access to $project2" in {
      Get(s"/views/$project2/documents/statistics") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    s"get statistics if the user has access to $project1" in {
      Get(s"/views/$project1/documents/statistics") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asString shouldEqual "indexing-statistics"
      }

      Get(s"/views/$project1/$encodedDefaultViewId/statistics") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asString shouldEqual "indexing-statistics"
      }
    }

    s"fail to get offset if the user has no access to $project2" in {
      Get(s"/views/$project2/documents/offset") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    s"get offset if the user has access to $project1" in {

      Get(s"/views/$project1/documents/offset") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asString shouldEqual "offset"
      }
    }

    s"fail to delete offset if the user has no write access to $project1" in {
      Delete(s"/views/$project1/documents/offset") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    s"get offset if the user has write access to $project1" in {

      Delete(s"/views/$project1/documents/offset") ~> as(writer) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asString shouldEqual "schedule-restart"
      }
    }

    s"fail perform a search if the user has no access to $project2" in {
      Post(s"/views/$project2/documents/_search", json"""{}""".toEntity) ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    s"return a search if the user has no access to $project1" in {
      Post(s"/views/$project1/documents/_search", json"""{}""".toEntity) ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asJson shouldEqual searchResult
      }

      Post(s"/views/$project1/$encodedDefaultViewId/_search", json"""{}""".toEntity) ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        response.asJson shouldEqual searchResult
      }
    }

    "return 404 when trying a segment different from the default view id" in {
      Get(s"/views/$project1/fail/statistics") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }

      Post(s"/views/$project1/fail/_search", json"""{}""".toEntity) ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }
}
