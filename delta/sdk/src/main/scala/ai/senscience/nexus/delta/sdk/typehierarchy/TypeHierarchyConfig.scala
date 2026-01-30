package ai.senscience.nexus.delta.sdk.typehierarchy

import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class TypeHierarchyConfig(eventLog: EventLogConfig)

object TypeHierarchyConfig {
  given ConfigReader[TypeHierarchyConfig] = deriveReader[TypeHierarchyConfig]
}
