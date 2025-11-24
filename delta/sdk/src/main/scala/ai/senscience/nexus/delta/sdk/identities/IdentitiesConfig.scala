package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.cache.CacheConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class IdentitiesConfig(fetchRemoteGroups: Boolean, cache: CacheConfig)

object IdentitiesConfig {

  given ConfigReader[IdentitiesConfig] = deriveReader[IdentitiesConfig]

}
