package ai.senscience.nexus.delta.sourcing.stream.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class BackpressureConfig(enabled: Boolean, bound: Int, maxElems: Int)

object BackpressureConfig {
  given ConfigReader[BackpressureConfig] = deriveReader[BackpressureConfig]
}
