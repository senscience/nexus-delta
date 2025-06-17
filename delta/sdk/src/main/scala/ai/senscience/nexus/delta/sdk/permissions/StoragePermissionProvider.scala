package ai.senscience.nexus.delta.sdk.permissions

import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sdk.permissions.StoragePermissionProvider.AccessType
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

/**
  * Provides the permission a user needs to have in order to access files on this storage
  */
trait StoragePermissionProvider {

  def permissionFor(id: IdSegmentRef, project: ProjectRef, accessType: AccessType): IO[Permission]

}

object StoragePermissionProvider {
  sealed trait AccessType
  object AccessType {
    case object Read  extends AccessType
    case object Write extends AccessType
  }
}
