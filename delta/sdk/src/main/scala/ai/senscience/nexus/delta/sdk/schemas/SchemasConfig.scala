package ai.senscience.nexus.delta.sdk.schemas

import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ch.epfl.bluebrain.nexus.delta.kernel.cache.CacheConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for the Schemas module.
  *
  * @param eventLog
  *   configuration of the event log
  */
final case class SchemasConfig(eventLog: EventLogConfig, cache: CacheConfig)

object SchemasConfig {
  implicit final val schemasConfigReader: ConfigReader[SchemasConfig] =
    deriveReader[SchemasConfig]
}
