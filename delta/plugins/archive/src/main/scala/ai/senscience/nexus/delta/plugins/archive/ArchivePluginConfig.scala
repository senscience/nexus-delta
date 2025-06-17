package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.sourcing.config.EphemeralLogConfig
import cats.effect.IO
import com.typesafe.config.Config
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}

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

  /**
    * Converts a [[Config]] into an [[ArchivePluginConfig]]
    */
  def load(config: Config): IO[ArchivePluginConfig] =
    IO.delay {
      ConfigSource
        .fromConfig(config)
        .at("plugins.archive")
        .loadOrThrow[ArchivePluginConfig]
    }

  implicit final val archivePluginConfigReader: ConfigReader[ArchivePluginConfig] =
    deriveReader[ArchivePluginConfig]
}
