package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.kernel.cache.CacheConfig
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for the ACLs module
  *
  * @param eventLog
  *   The event log configuration
  * @param provisioning
  *   The provisioning
  * @param cache
  *   Cache acls for authentication operations
  * @param enableOwnerPermissions
  *   Enable the creation of owner permissions when
  */
final case class AclsConfig(
    eventLog: EventLogConfig,
    provisioning: AclProvisioningConfig,
    cache: CacheConfig,
    enableOwnerPermissions: Boolean
)

object AclsConfig {
  given ConfigReader[AclsConfig] = deriveReader[AclsConfig]
}
