package ai.senscience.nexus.delta.plugins.blazegraph.config

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlTarget
import ai.senscience.nexus.delta.sdk.Defaults
import ai.senscience.nexus.delta.sdk.instances.*
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import org.http4s.{BasicCredentials, Uri}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.*
import pureconfig.module.http4s.*

import scala.concurrent.duration.*

/**
  * Configuration for the Blazegraph views module.
  *
  * @param base
  *   the base uri to the Blazegraph HTTP endpoint
  * @param credentials
  *   the Blazegraph HTTP endpoint credentials
  * @param queryTimeout
  *   the Blazegraph query timeout
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
    base: Uri,
    sparqlTarget: SparqlTarget,
    credentials: Option[BasicCredentials],
    queryTimeout: Duration,
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

  implicit final val blazegraphViewsConfigConfigReader: ConfigReader[BlazegraphViewsConfig] =
    deriveReader[BlazegraphViewsConfig]
}
