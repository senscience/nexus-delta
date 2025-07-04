package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.sdk.acls.model.{AclAddress, FlattenedAclStore}
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.ProjectScopeResolver.PermissionAccess
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import fs2.Stream

/**
  * Allows to cross acls and projects
  */
trait ProjectScopeResolver {

  /**
    * Get the available projects where the current user has the given permission for the given scope
    */
  def apply(scope: Scope, permission: Permission)(implicit caller: Caller): IO[Set[ProjectRef]]

  /**
    * Gives the list of addresses where the current user has the given permission for the given scope
    */
  def access(scope: Scope, permission: Permission)(implicit caller: Caller): IO[PermissionAccess]
}

object ProjectScopeResolver {

  final case class PermissionAccess(permission: Permission, authorizedAddresses: Set[AclAddress]) {
    def grant(project: ProjectRef): Boolean =
      authorizedAddresses.contains(AclAddress.Root) ||
        AclAddress.Project(project).ancestors.exists(authorizedAddresses.contains)
  }

  def apply(projects: Projects, flattenedAclStore: FlattenedAclStore): ProjectScopeResolver =
    apply(projects.currentRefs(_), flattenedAclStore)

  def apply(
      fetchProjects: Scope => Stream[IO, ProjectRef],
      flattenedAclStore: FlattenedAclStore
  ): ProjectScopeResolver =
    new ProjectScopeResolver {
      override def apply(scope: Scope, permission: Permission)(implicit caller: Caller): IO[Set[ProjectRef]] = {
        access(scope, permission).flatMap { permissionAccess =>
          fetchProjects(scope).filter(permissionAccess.grant).compile.to(Set).flatTap { authorizedProjects =>
            IO.raiseWhen(authorizedProjects.isEmpty)(AuthorizationFailed("No projects are accessible."))
          }
        }
      }

      override def access(scope: Scope, permission: Permission)(implicit caller: Caller): IO[PermissionAccess] = {
        val address = AclAddress.fromScope(scope)
        flattenedAclStore.fetchAddresses(address, permission, caller.identities).map(PermissionAccess(permission, _))
      }
    }

}
