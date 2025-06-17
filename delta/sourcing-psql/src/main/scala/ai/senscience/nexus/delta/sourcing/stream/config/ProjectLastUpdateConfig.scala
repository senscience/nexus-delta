package ai.senscience.nexus.delta.sourcing.stream.config

import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

final case class ProjectLastUpdateConfig(batch: BatchConfig, query: QueryConfig, inactiveInterval: FiniteDuration)

object ProjectLastUpdateConfig {
  implicit final val projectLastUpdateConfig: ConfigReader[ProjectLastUpdateConfig] =
    deriveReader[ProjectLastUpdateConfig]
}
