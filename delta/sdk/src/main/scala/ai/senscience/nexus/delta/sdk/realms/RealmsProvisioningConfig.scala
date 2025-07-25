package ai.senscience.nexus.delta.sdk.realms

import ai.senscience.nexus.delta.sdk.realms.model.RealmFields
import ai.senscience.nexus.delta.sourcing.model.Label
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration to provision realms
  * @param enabled
  *   flag to enable provisioning at startup
  * @param realms
  *   the collection of realms to create
  */
final case class RealmsProvisioningConfig(enabled: Boolean, realms: Map[Label, RealmFields])

object RealmsProvisioningConfig {

  implicit private val mapReader: ConfigReader[Map[Label, RealmFields]] = Label.labelMapReader[RealmFields]

  implicit final val realmsProvisioningConfigReader: ConfigReader[RealmsProvisioningConfig] =
    deriveReader[RealmsProvisioningConfig]

}
