package ai.senscience.nexus.delta.sdk.sse

import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class SseConfig(query: QueryConfig)

object SseConfig {

  given ConfigReader[SseConfig] = deriveReader[SseConfig]
}
