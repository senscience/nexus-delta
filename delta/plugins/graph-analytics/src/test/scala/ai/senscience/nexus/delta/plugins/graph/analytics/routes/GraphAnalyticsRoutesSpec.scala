package ai.senscience.nexus.delta.plugins.graph.analytics.routes

import ai.senscience.nexus.delta.plugins.graph.analytics.model.AnalyticsGraph.{Edge, EdgePath, Node}
import ai.senscience.nexus.delta.plugins.graph.analytics.model.PropertiesStatistics.Metadata
import ai.senscience.nexus.delta.plugins.graph.analytics.model.{AnalyticsGraph, PropertiesStatistics}
import ai.senscience.nexus.delta.plugins.graph.analytics.{contexts, permissions, GraphAnalytics}
import ai.senscience.nexus.delta.rdf.Vocabulary.schema
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.ProjectNotFound
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route
import org.scalatest.CancelAfterFailure

class GraphAnalyticsRoutesSpec extends BaseRouteSpec with CancelAfterFailure {

  override def extraContexts: RemoteContextResolution = loadCoreContexts(contexts.definition)

  private val identities = IdentitiesDummy.fromUsers(alice)

  private val aclCheck = AclSimpleCheck().accepted
  private val project  = ProjectRef.unsafe("org", "project")

  private def projectNotFound(projectRef: ProjectRef) = ProjectNotFound(projectRef).asInstanceOf[ProjectRejection]

  private val graphAnalytics = new GraphAnalytics {

    override def relationships(projectRef: ProjectRef): IO[AnalyticsGraph] =
      IO.raiseWhen(projectRef != project)(projectNotFound(projectRef))
        .as(
          AnalyticsGraph(
            nodes = List(Node(schema.Person, "Person", 10), Node(schema + "Address", "Address", 5)),
            edges = List(Edge(schema.Person, schema + "Address", 5, Seq(EdgePath(schema + "address", "address"))))
          )
        )

    override def properties(projectRef: ProjectRef, tpe: IdSegment): IO[PropertiesStatistics] =
      IO.raiseWhen(projectRef != project)(projectNotFound(projectRef))
        .as(
          PropertiesStatistics(
            Metadata(schema.Person, "Person", 10),
            Seq(PropertiesStatistics(Metadata(schema + "address", "address", 5), Seq.empty))
          )
        )
  }

  private val viewQueryResponse = json"""{"key": "value"}"""

  private lazy val routes =
    Route.seal(
      new GraphAnalyticsRoutes(
        identities,
        aclCheck,
        graphAnalytics,
        ProjectionsDirectives.testEcho,
        (_, _) => IO.pure(viewQueryResponse)
      ).routes
    )

  "graph analytics routes" when {

    "dealing with relationships" should {
      "fail to fetch without resources/read permission" in {
        aclCheck.append(AclAddress.Root, alice -> Set(resources.read)).accepted
        Get("/v1/graph-analytics/org/project/relationships") ~> routes ~> check {
          response.shouldBeForbidden
        }
      }

      "fetch" in {
        Get("/v1/graph-analytics/org/project/relationships") ~> as(alice) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual jsonContentOf("routes/relationships.json")
        }
      }
    }

    "dealing with properties" should {

      "fail to fetch without resources/read permission" in {
        Get("/v1/graph-analytics/org/project/properties/Person") ~> routes ~> check {
          response.shouldBeForbidden
        }
      }

      "fetch" in {
        Get("/v1/graph-analytics/org/project/properties/Person") ~> as(alice) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual jsonContentOf("routes/properties.json")
        }
      }
    }

    "dealing with stream progress" should {

      "fail to fetch without resources/read permission" in {
        Get("/v1/graph-analytics/org/project/statistics") ~> routes ~> check {
          response.shouldBeForbidden
        }
      }

      "fetch" in {
        Get("/v1/graph-analytics/org/project/statistics") ~> as(alice) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asString shouldEqual "indexing-statistics"
        }
      }
    }

    "querying" should {

      val query = json"""{ "query": { "match_all": {} } }"""

      "fail without authorization" in {
        Post("/v1/graph-analytics/org/project/_search", query.toEntity) ~> as(alice) ~> routes ~> check {
          response.shouldBeForbidden
        }
      }

      "succeed" in {
        aclCheck.append(AclAddress.Root, alice -> Set(permissions.query)).accepted
        Post("/v1/graph-analytics/org/project/_search", query.toEntity) ~> as(alice) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual viewQueryResponse
        }
      }

    }

  }

}
