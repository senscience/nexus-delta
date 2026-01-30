package ai.senscience.nexus.delta.plugins.storage

import ai.senscience.nexus.delta.plugins.storage.files.FilesConfig
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig
import ai.senscience.nexus.delta.sdk.Defaults
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class StoragePluginConfig(
    enableDefaultCreation: Boolean,
    storages: StoragesConfig,
    files: FilesConfig,
    defaults: Defaults
)

object StoragePluginConfig {

  given ConfigReader[StoragePluginConfig] = deriveReader[StoragePluginConfig]
}
