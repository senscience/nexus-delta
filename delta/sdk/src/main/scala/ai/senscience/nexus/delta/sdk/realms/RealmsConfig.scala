package ai.senscience.nexus.delta.sdk.realms

import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for the Realms module.
  *
  * @param eventLog
  *   The event log configuration
  * @param pagination
  *   configuration for how pagination should behave in listing operations
  * @param provisioning
  *   configuration to provision realms at startup
  */
final case class RealmsConfig(
    eventLog: EventLogConfig,
    pagination: PaginationConfig,
    provisioning: RealmsProvisioningConfig
)

object RealmsConfig {
  given ConfigReader[RealmsConfig] = deriveReader[RealmsConfig]
}
