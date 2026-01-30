package ai.senscience.nexus.delta.plugins.storage.files

import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for the files module.
  *
  * @param eventLog
  *   configuration of the event log
  */
final case class FilesConfig(eventLog: EventLogConfig, mediaTypeDetector: MediaTypeDetectorConfig)

object FilesConfig {
  given ConfigReader[FilesConfig] = deriveReader[FilesConfig]
}
