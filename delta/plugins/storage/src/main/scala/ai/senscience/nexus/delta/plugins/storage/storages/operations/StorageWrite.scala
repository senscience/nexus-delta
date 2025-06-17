package ai.senscience.nexus.delta.plugins.storage.storages.operations

import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageType
import ai.senscience.nexus.delta.sourcing.model.ResourceRef

/**
  * Result on a write operation on a storage
  *
  * @param storage
  *   the reference of the storage
  * @param tpe
  *   its type
  * @param value
  *   the value returned by the operation
  */
final case class StorageWrite[A](storage: ResourceRef.Revision, tpe: StorageType, value: A)
