package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sdk.permissions.StoragePermissionProvider
import ai.senscience.nexus.delta.sdk.permissions.StoragePermissionProvider.AccessType.{Read, Write}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

class StoragePermissionProviderImpl(storages: Storages) extends StoragePermissionProvider {
  override def permissionFor(
      id: IdSegmentRef,
      project: ProjectRef,
      accessType: StoragePermissionProvider.AccessType
  ): IO[Permission] = {
    storages
      .fetch(id, project)
      .map(storage => storage.value.storageValue)
      .map(storage =>
        accessType match {
          case Read  => storage.readPermission
          case Write => storage.writePermission
        }
      )
  }
}
