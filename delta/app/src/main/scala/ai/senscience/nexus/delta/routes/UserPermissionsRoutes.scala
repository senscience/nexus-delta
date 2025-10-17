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
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.effect.unsafe.implicits.*
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
final class UserPermissionsRoutes(identities: Identities, aclCheck: AclCheck, storages: StoragePermissionProvider)(
    implicit baseUri: BaseUri
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  given Tracer[IO] = Tracer.noop

  private def fetchStoragePermission(project: ProjectRef, storageId: IdSegment, accessType: AccessType) =
    onSuccess(storages.permissionFor(IdSegmentRef(storageId), project, accessType).unsafeToFuture())

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("user") {
        pathPrefix("permissions") {
          projectRef { project =>
            extractCaller { implicit caller =>
              head {
                concat(
                  parameter("permission".as[Permission]) { permission =>
                    authorizeFor(project, permission) {
                      complete(StatusCodes.NoContent)
                    }
                  },
                  parameters("storage".as[IdSegment], "type".as[AccessType]) { (storageId, accessType) =>
                    fetchStoragePermission(project, storageId, accessType) { permission =>
                      authorizeFor(project, permission) {
                        complete(StatusCodes.NoContent)
                      }
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
