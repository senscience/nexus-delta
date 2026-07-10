package ai.senscience.nexus.delta.elasticsearch.config

import ai.senscience.nexus.delta.elasticsearch.client.Refresh
import ai.senscience.nexus.delta.elasticsearch.config.ElasticSearchViewsConfig.OpentelemetryConfig
import ai.senscience.nexus.delta.kernel.RetryStrategyConfig
import ai.senscience.nexus.delta.kernel.http.client.middleware.HttpAuth
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sourcing.config.{EventLogConfig, QueryConfig}
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import org.http4s.Uri
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
  * @param serverless
  *   whether the targeted Elasticsearch is a serverless deployment; when true, index settings unsupported on serverless
  *   (e.g. `number_of_shards`) are omitted
  */
final case class ElasticSearchViewsConfig(
    base: Uri,
    credentials: HttpAuth,
    eventLog: EventLogConfig,
    pagination: PaginationConfig,
    retryStrategy: RetryStrategyConfig,
    batch: BatchConfig,
    prefix: String,
    mainIndex: MainIndexConfig,
    maxViewRefs: Int,
    syncIndexingTimeout: FiniteDuration,
    syncIndexingRefresh: Refresh,
    maxIndexPathLength: Int,
    metricsQuery: QueryConfig,
    indexingEnabled: Boolean,
    serverless: Boolean,
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

  // Serverless only accepts api-key auth; api-key auth is also valid against self-managed/hosted, so only enforce the
  // mandatory direction: serverless => api-key.
  given ConfigReader[ElasticSearchViewsConfig] =
    deriveReader[ElasticSearchViewsConfig].emap { cfg =>
      val usesApiKey = cfg.credentials match {
        case _: HttpAuth.ApiKey => true
        case _                  => false
      }
      Either.cond(
        !cfg.serverless || usesApiKey,
        cfg,
        CannotConvert(
          s"serverless = ${cfg.serverless}",
          classOf[ElasticSearchViewsConfig].getSimpleName,
          "'serverless = true' requires api-key credentials"
        )
      )
    }
}
