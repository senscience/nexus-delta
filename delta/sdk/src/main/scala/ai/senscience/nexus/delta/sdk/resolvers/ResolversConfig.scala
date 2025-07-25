package ai.senscience.nexus.delta.sdk.resolvers

import ai.senscience.nexus.delta.sdk.Defaults
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for the Resolvers module.
  *
  * @param eventLog
  *   configuration of the event log
  */
final case class ResolversConfig(
    eventLog: EventLogConfig,
    defaults: Defaults
)

object ResolversConfig {
  implicit final val resolversConfigReader: ConfigReader[ResolversConfig] =
    deriveReader[ResolversConfig]
}
