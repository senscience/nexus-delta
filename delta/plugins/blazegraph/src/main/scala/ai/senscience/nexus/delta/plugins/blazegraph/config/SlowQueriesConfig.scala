package ai.senscience.nexus.delta.plugins.blazegraph.config

import ai.senscience.nexus.delta.sourcing.config.PurgeConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.*

/**
  * Configuration for the Blazegraph slow queries logging.
  *
  * @param slowQueryThreshold
  *   how long a query takes before it is considered slow
  * @param logTtl
  *   how long to keep logged slow queries
  * @param deleteExpiredLogsEvery
  *   how often to delete expired logs
  */
final case class SlowQueriesConfig(
    slowQueryThreshold: Duration,
    logTtl: FiniteDuration,
    deleteExpiredLogsEvery: FiniteDuration
) {
  def purge: PurgeConfig = PurgeConfig(deleteExpiredLogsEvery, logTtl)
}

object SlowQueriesConfig {
  implicit final val eventLogConfig: ConfigReader[SlowQueriesConfig] =
    deriveReader[SlowQueriesConfig]
}
