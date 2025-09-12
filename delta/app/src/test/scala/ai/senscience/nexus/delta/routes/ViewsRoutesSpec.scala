package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources.read
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sdk.views.ViewsList.AggregateViewsList
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import org.apache.pekko.http.scaladsl.server.Route

class ViewsRoutesSpec extends BaseRouteSpec {

  private val project = ProjectRef.unsafe("org", "proj")

  private val aclCheck = AclSimpleCheck.unsafe(
    (alice, AclAddress.Project(project), Set(read))
  )

  private val viewsList = new AggregateViewsList(List.empty)

  private lazy val routes = Route.seal(
    new ViewsRoutes(
      IdentitiesDummy.fromUsers(alice),
      aclCheck,
      viewsList
    ).routes
  )

  "The views list routes" should {
    s"fail without the '$read' permission on the provided project" in {
      Get("/v1/views/xxx/xxx") ~> as(alice) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    s"succeed with the '$read' permission on the provided project" in {
      Get("/v1/views/xxx/xxx") ~> as(alice) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }
  }

}
