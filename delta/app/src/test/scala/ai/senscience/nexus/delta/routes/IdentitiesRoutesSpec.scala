package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.marshalling.RdfExceptionHandler
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import org.apache.pekko.http.scaladsl.model.MediaRanges.`*/*`
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.{Accept, BasicHttpCredentials, OAuth2BearerToken}
import org.apache.pekko.http.scaladsl.server.Directives.handleExceptions
import org.apache.pekko.http.scaladsl.server.Route

class IdentitiesRoutesSpec extends BaseRouteSpec {

  private val identities = IdentitiesDummy.fromUsers(alice)

  private val aclCheck = AclSimpleCheck().accepted

  private val route = Route.seal(
    handleExceptions(RdfExceptionHandler.apply) {
      IdentitiesRoutes(identities, aclCheck)
    }
  )

  "The identity routes" should {
    "return forbidden" in {
      Get("/v1/identities") ~> addCredentials(OAuth2BearerToken("unknown")) ~> route ~> check {
        status shouldEqual StatusCodes.Unauthorized
      }
    }

    "return unauthorized" in {
      Get("/v1/identities") ~> addCredentials(BasicHttpCredentials("fail")) ~> route ~> check {
        status shouldEqual StatusCodes.Unauthorized
      }
    }

    "return anonymous" in {
      Get("/v1/identities") ~> Accept(`*/*`) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        response.asJson should equalIgnoreArrayOrder(jsonContentOf("identities/anonymous.json"))
      }
    }

    "return all identities" in {
      Get("/v1/identities") ~> Accept(`*/*`) ~> addCredentials(OAuth2BearerToken("alice")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        response.asJson should equalIgnoreArrayOrder(jsonContentOf("identities/alice.json"))
      }
    }
  }
}
