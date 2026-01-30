package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.jwt.AuthToken
import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.InvalidAccessToken
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.RemoteContextResolutionFixtures
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.identities.model.Caller.Anonymous
import ai.senscience.nexus.delta.sdk.marshalling.RdfExceptionHandler
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.scalatest.BaseSpec
import ai.senscience.nexus.testkit.scalatest.ce.{CatsEffectSpec, CatsIOValues}
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.{BasicHttpCredentials, OAuth2BearerToken}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, Route}
import org.scalatest.matchers.should.Matchers

class AuthDirectivesSpec
    extends BaseSpec
    with RouteHelpers
    with CatsEffectSpec
    with Matchers
    with CatsIOValues
    with RemoteContextResolutionFixtures {

  private given BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private given RemoteContextResolution = loadCoreContexts(Set.empty)

  private given JsonKeyOrdering =
    JsonKeyOrdering.default(topKeys = List("@context", "@id", "@type", "reason", "details"))

  val user: Subject = User("alice", Label.unsafe("wonderland"))
  val userCaller    = Caller(user, Set(user))
  val user2         = User("bob", Label.unsafe("wonderland"))
  val user2Caller   = Caller(user2, Set(user2))

  val permission = Permission.unsafe("test/read")

  val identities = new Identities {

    override def exchange(token: AuthToken): IO[Caller] = {
      token match {
        case AuthToken("alice") => IO.pure(userCaller)
        case AuthToken("bob")   => IO.pure(user2Caller)
        case _                  => IO.raiseError(InvalidAccessToken("John", "Doe", "Expired JWT"))

      }
    }
  }

  val aclCheck = AclSimpleCheck((user, AclAddress.Root, Set(permission))).accepted

  val directives = new AuthDirectives(identities, aclCheck) {}

  private val callerRoute: Route =
    handleExceptions(RdfExceptionHandler.apply) {
      path("user") {
        directives.extractCaller { caller =>
          get {
            caller match {
              case Anonymous             => complete("anonymous")
              case Caller(user: User, _) => complete(user.subject)
              case _                     => complete("another")
            }

          }
        }
      }
    }

  private val authExceptionHandler: ExceptionHandler = ExceptionHandler { case AuthorizationFailed(_) =>
    complete(StatusCodes.Forbidden)
  }

  private val authorizationRoute: Route =
    handleExceptions(authExceptionHandler) {
      path("user") {
        directives.extractCaller { caller =>
          directives.authorizeFor(AclAddress.Root, permission)(using caller).apply {
            get {
              caller match {
                case Anonymous             => complete("anonymous")
                case Caller(user: User, _) => complete(user.subject)
                case _                     => complete("another")
              }
            }
          }
        }
      }
    }

  "A route" should {

    "correctly extract the user Alice" in {
      Get("/user") ~> addCredentials(OAuth2BearerToken("alice")) ~> callerRoute ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asString shouldEqual "alice"
      }
    }

    "correctly extract Anonymous" in {
      Get("/user") ~> callerRoute ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asString shouldEqual "anonymous"
      }
    }

    "fail with an invalid token" in {
      Get("/user") ~> addCredentials(OAuth2BearerToken("unknown")) ~> callerRoute ~> check {
        response.status shouldEqual StatusCodes.Unauthorized
        response.asJson shouldEqual jsonContentOf(
          "identities/invalid-access-token.json",
          "subject" -> "John",
          "issuer"  -> "Doe"
        )
      }
    }

    "fail with invalid credentials" in {
      Get("/user") ~> addCredentials(BasicHttpCredentials("alice")) ~> callerRoute ~> check {
        response.status shouldEqual StatusCodes.Unauthorized
        response.asJson shouldEqual jsonContentOf("identities/authentication-failed.json")
      }
    }

    "correctly authorize user" in {
      Get("/user") ~> addCredentials(OAuth2BearerToken("alice")) ~> authorizationRoute ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asString shouldEqual "alice"
      }
    }

    "correctly reject Anonymous " in {
      Get("/user") ~> authorizationRoute ~> check {
        response.status shouldEqual StatusCodes.Forbidden
      }
    }

    "correctly reject user without permission " in {
      Get("/user") ~> addCredentials(OAuth2BearerToken("bob")) ~> authorizationRoute ~> check {
        response.status shouldEqual StatusCodes.Forbidden
      }
    }
  }
}
