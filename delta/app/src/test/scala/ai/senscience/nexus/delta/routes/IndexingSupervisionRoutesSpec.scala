package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.permissions.Permissions.{projects, supervision}
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.projections.ProjectionErrors
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route

class IndexingSupervisionRoutesSpec extends BaseRouteSpec {

  private val supervisor = User("supervisor", realm)

  private lazy val projectionErrors = ProjectionErrors(xas, queryConfig, clock)

  private val identities = IdentitiesDummy.fromUsers(supervisor)
  private val aclCheck   = AclSimpleCheck.unsafe(
    (supervisor, AclAddress.Root, Set(supervision.read, projects.write))
  )

  private lazy val routes = Route.seal(
    new IndexingSupervisionRoutes(
      identities,
      aclCheck,
      projectionErrors
    ).routes
  )

  "Fail to access the indexing error count if the user has no access" in {
    Get("/v1/supervision/indexing/errors/count") ~> routes ~> check {
      response.status shouldEqual StatusCodes.Forbidden
    }
  }

  "Succeed to get the indexing error count if the user has access" in {
    Get("/v1/supervision/indexing/errors/count") ~> as(supervisor) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual json"""0"""
    }
  }

  "Fail to access the indexing error latest if the user has no access" in {
    Get("/v1/supervision/indexing/errors/latest") ~> routes ~> check {
      response.status shouldEqual StatusCodes.Forbidden
    }
  }

  "Succeed to get the indexing error latest if the user has access" in {
    Get("/v1/supervision/indexing/errors/latest") ~> as(supervisor) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual json"""{ "errors": [] }"""
    }
  }

}
