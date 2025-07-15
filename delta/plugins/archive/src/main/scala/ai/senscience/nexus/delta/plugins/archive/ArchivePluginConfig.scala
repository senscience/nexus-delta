package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.sourcing.config.EphemeralLogConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Archive plugin configuration.
  *
  * @param priority
  *   the plugin priority
  * @param ephemeral
  *   the ephemeral configuration
  */
final case class ArchivePluginConfig(
    priority: Int,
    ephemeral: EphemeralLogConfig
)
object ArchivePluginConfig {

  implicit final val archivePluginConfigReader: ConfigReader[ArchivePluginConfig] =
    deriveReader[ArchivePluginConfig]
}
