package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment, IdSegmentRef}
import ai.senscience.nexus.delta.sdk.permissions.StoragePermissionProvider
import ai.senscience.nexus.delta.sdk.permissions.StoragePermissionProvider.AccessType
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route

/**
  * The user permissions routes. Used for checking whether the current logged in user has certain permissions.
  *
  * @param identities
  *   the identities operations bundle
  * @param aclCheck
  *   verify the acls for users
  */
final class UserPermissionsRoutes(identities: Identities, aclCheck: AclCheck, storages: StoragePermissionProvider)(
    implicit baseUri: BaseUri
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("user") {
        pathPrefix("permissions") {
          projectRef { project =>
            extractCaller { implicit caller =>
              head {
                concat(
                  parameter("permission".as[Permission]) { permission =>
                    authorizeFor(project, permission)(caller) {
                      complete(StatusCodes.NoContent)
                    }
                  },
                  parameters("storage".as[IdSegment], "type".as[AccessType]) { (storageId, `type`) =>
                    authorizeForIO(
                      AclAddress.fromProject(project),
                      storages.permissionFor(IdSegmentRef(storageId), project, `type`)
                    )(caller) {
                      complete(StatusCodes.NoContent)
                    }
                  }
                )
              }
            }
          }
        }
      }
    }
}

object UserPermissionsRoutes {
  def apply(identities: Identities, aclCheck: AclCheck, storagePermissionProvider: StoragePermissionProvider)(implicit
      baseUri: BaseUri
  ): Route =
    new UserPermissionsRoutes(identities, aclCheck: AclCheck, storagePermissionProvider).routes
}
