package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.sdk.instances.given
import fs2.io.file.Path
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class AclProvisioningConfig(enabled: Boolean, path: Option[Path])

object AclProvisioningConfig {
  given ConfigReader[AclProvisioningConfig] = deriveReader[AclProvisioningConfig]
}
