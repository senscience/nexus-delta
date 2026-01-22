package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

/**
  * The user permissions routes. Used for checking whether the current logged in user has certain permissions.
  *
  * @param identities
  *   the identities operations bundle
  * @param aclCheck
  *   verify the acls for users
  */
final class UserPermissionsRoutes(identities: Identities, aclCheck: AclCheck)(using baseUri: BaseUri)
    extends AuthDirectives(identities, aclCheck) {

  given Tracer[IO] = Tracer.noop

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      (pathPrefix("user") & pathPrefix("permissions")) {
        projectRef { project =>
          extractCaller { case given Caller =>
            head {
              parameter("permission".as[Permission]) { permission =>
                authorizeFor(project, permission) {
                  complete(StatusCodes.NoContent)
                }
              }
            }
          }
        }
      }
    }
}

object UserPermissionsRoutes {
  def apply(identities: Identities, aclCheck: AclCheck)(using baseUri: BaseUri): Route =
    new UserPermissionsRoutes(identities, aclCheck: AclCheck).routes
}
