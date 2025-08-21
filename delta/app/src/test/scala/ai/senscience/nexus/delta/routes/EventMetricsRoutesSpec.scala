package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.permissions.Permissions.{projects, supervision}
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route

class EventMetricsRoutesSpec extends BaseRouteSpec {

  private val supervisor = User("supervisor", realm)

  private val identities = IdentitiesDummy.fromUsers(supervisor)
  private val aclCheck   = AclSimpleCheck.unsafe(
    (supervisor, AclAddress.Root, Set(supervision.read, projects.write))
  )

  private lazy val routes = Route.seal(
    new EventMetricsRoutes(
      identities,
      aclCheck,
      ProjectionsDirectives.testEcho
    ).routes
  )

  "Fail to access statistics if the user has no access" in {
    Get("/v1/event-metrics/statistics") ~> routes ~> check {
      response.status shouldEqual StatusCodes.Forbidden
    }
  }

  "Succeed to get the statistics if the user has access" in {
    Get("/v1/event-metrics/statistics") ~> as(supervisor) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "indexing-statistics"
    }
  }

  "Fail to access the indexing failures count if the user has no access" in {
    Get("/v1/event-metrics/failures") ~> routes ~> check {
      response.status shouldEqual StatusCodes.Forbidden
    }
  }

  "Succeed to get the indexing failures if the user has access" in {
    Get("/v1/event-metrics/failures") ~> as(supervisor) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "indexing-errors"
    }
  }

  "Fail to access offset if the user has no access" in {
    Get("/v1/event-metrics/offset") ~> routes ~> check {
      response.status shouldEqual StatusCodes.Forbidden
    }
  }

  "Succeed to get the offset if the user has access" in {
    Get("/v1/event-metrics/offset") ~> as(supervisor) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "offset"
    }
  }

  "Fail to restart indexing if the user has no access" in {
    Delete("/v1/event-metrics/offset") ~> routes ~> check {
      response.status shouldEqual StatusCodes.Forbidden
    }
  }

  "Succeed to restart indexing if the user has access" in {
    Delete("/v1/event-metrics/offset") ~> as(supervisor) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "schedule-restart"
    }
  }

}
