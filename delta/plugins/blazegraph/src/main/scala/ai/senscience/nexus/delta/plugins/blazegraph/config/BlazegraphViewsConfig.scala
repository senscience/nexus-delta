package ai.senscience.nexus.delta.plugins.blazegraph.config

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig
import ai.senscience.nexus.delta.sdk.Defaults
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.*

import scala.concurrent.duration.*

/**
  * Configuration for the Blazegraph views module.
  *
  * @param access
  *   the configuration of the sparql instances
  * @param eventLog
  *   configuration of the event log
  * @param pagination
  *   configuration for how pagination should behave in listing operations
  * @param prefix
  *   prefix for namespaces
  * @param maxViewRefs
  *   configuration of the maximum number of view references allowed on an aggregated view
  * @param syncIndexingTimeout
  *   the maximum duration for synchronous indexing to complete
  * @param defaults
  *   default values for the default Blazegraph views
  * @param indexingEnabled
  *   if false, disables Blazegraph indexing
  */
final case class BlazegraphViewsConfig(
    access: SparqlAccess,
    eventLog: EventLogConfig,
    pagination: PaginationConfig,
    batch: BatchConfig,
    retryStrategy: RetryStrategyConfig,
    prefix: String,
    maxViewRefs: Int,
    syncIndexingTimeout: FiniteDuration,
    defaults: Defaults,
    indexingEnabled: Boolean
)

object BlazegraphViewsConfig {

  given ConfigReader[BlazegraphViewsConfig] =
    deriveReader[BlazegraphViewsConfig]

  final case class OpentelemetryConfig(captureQueries: Boolean)

  object OpentelemetryConfig {
    given ConfigReader[OpentelemetryConfig] = deriveReader[OpentelemetryConfig]
  }
}
