package ai.senscience.nexus.delta.elasticsearch.config

import ai.senscience.nexus.delta.elasticsearch.client.Refresh
import ai.senscience.nexus.delta.elasticsearch.config.ElasticSearchViewsConfig.OpentelemetryConfig
import ai.senscience.nexus.delta.sdk.instances.*
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sourcing.config.{EventLogConfig, QueryConfig}
import ai.senscience.nexus.delta.sourcing.stream.config.{BackpressureConfig, BatchConfig}
import org.http4s.{BasicCredentials, Uri}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.deriveReader
import pureconfig.module.http4s.*

import scala.concurrent.duration.*

/**
  * Configuration for the ElasticSearchView plugin.
  *
  * @param base
  *   the base uri to the Elasticsearch HTTP endpoint
  * @param credentials
  *   the credentials to authenticate to the Elasticsearch endpoint
  * @param eventLog
  *   configuration of the event log
  * @param pagination
  *   configuration for how pagination should behave in listing operations
  * @param batch
  *   a configuration definition how often we want to push to Elasticsearch
  * @param prefix
  *   prefix for indices
  * @param maxViewRefs
  *   configuration of the maximum number of view references allowed on an aggregated view
  * @param syncIndexingTimeout
  *   the maximum duration for synchronous indexing to complete
  * @param syncIndexingRefresh
  *   the value for `refresh` Elasticsearch parameter for synchronous indexing
  * @param maxIndexPathLength
  *   the maximum length of the URL path for Elasticsearch queries
  * @param metricsQuery
  *   query configuration for the metrics projection
  * @param indexingEnabled
  *   if false, disables Elasticsearch indexing
  */
final case class ElasticSearchViewsConfig(
    base: Uri,
    credentials: Option[BasicCredentials],
    eventLog: EventLogConfig,
    pagination: PaginationConfig,
    batch: BatchConfig,
    backpressure: BackpressureConfig,
    prefix: String,
    mainIndex: MainIndexConfig,
    maxViewRefs: Int,
    syncIndexingTimeout: FiniteDuration,
    syncIndexingRefresh: Refresh,
    maxIndexPathLength: Int,
    metricsQuery: QueryConfig,
    indexingEnabled: Boolean,
    otel: OpentelemetryConfig
)

object ElasticSearchViewsConfig {

  private given ConfigReader[Refresh] = ConfigReader.fromString {
    case "true"     => Right(Refresh.True)
    case "false"    => Right(Refresh.False)
    case "wait_for" => Right(Refresh.WaitFor)
    case other      =>
      Left(
        CannotConvert(
          other,
          classOf[Refresh].getSimpleName,
          s"Incorrect value for 'refresh' parameter, allowed values are 'true', 'false', 'wait_for', got '$other' instead."
        )
      )
  }

  final case class OpentelemetryConfig(captureQueries: Boolean)

  object OpentelemetryConfig {
    given ConfigReader[OpentelemetryConfig] = deriveReader[OpentelemetryConfig]
  }

  given ConfigReader[ElasticSearchViewsConfig] =
    deriveReader[ElasticSearchViewsConfig]
}
