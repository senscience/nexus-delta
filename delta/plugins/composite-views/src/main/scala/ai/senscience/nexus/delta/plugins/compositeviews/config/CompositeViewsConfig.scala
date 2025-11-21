package ai.senscience.nexus.delta.plugins.compositeviews.config

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig
import ai.senscience.nexus.delta.plugins.blazegraph.config.SparqlAccess
import ai.senscience.nexus.delta.plugins.compositeviews.config.CompositeViewsConfig.SinkConfig.SinkConfig
import ai.senscience.nexus.delta.plugins.compositeviews.config.CompositeViewsConfig.{RemoteSourceClientConfig, SourcesConfig}
import ai.senscience.nexus.delta.sdk.auth
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.stream.config.{BackpressureConfig, BatchConfig}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.*

import scala.concurrent.duration.FiniteDuration

/**
  * The composite view configuration.
  *
  * @param sources
  *   the configuration of the composite views sources
  * @param sparqlAccess
  *   the configuration of the Blazegraph instance used for composite views
  * @param prefix
  *   prefix for indices and namespaces
  * @param maxProjections
  *   maximum number of projections allowed
  * @param eventLog
  *   configuration of the event log
  * @param pagination
  *   pagination config
  * @param remoteSourceClient
  *   the HTTP client configuration for a remote source
  * @param minIntervalRebuild
  *   the minimum allowed value for periodic rebuild strategy
  * @param blazegraphBatch
  *   the batch configuration for indexing into the blazegraph common space and the blazegraph projections
  * @param elasticsearchBatch
  *   the batch configuration for indexing into the elasticsearch projections
  * @param restartCheckInterval
  *   the interval at which a view will look for requested restarts
  * @param indexingEnabled
  *   if false, disables composite view indexing
  * @param sinkConfig
  *   type of sink used for composite indexing
  */
final case class CompositeViewsConfig(
    sources: SourcesConfig,
    sparqlAccess: SparqlAccess,
    prefix: String,
    maxProjections: Int,
    eventLog: EventLogConfig,
    pagination: PaginationConfig,
    remoteSourceClient: RemoteSourceClientConfig,
    minIntervalRebuild: FiniteDuration,
    blazegraphBatch: BatchConfig,
    elasticsearchBatch: BatchConfig,
    backpressure: BackpressureConfig,
    retryStrategy: RetryStrategyConfig,
    restartCheckInterval: FiniteDuration,
    indexingEnabled: Boolean,
    sinkConfig: SinkConfig,
    remoteSourceCredentials: auth.Credentials
)

object CompositeViewsConfig {

  /**
    * The sources configuration
    *
    * @param maxSources
    *   maximum number of sources allowed
    */
  final case class SourcesConfig(
      maxSources: Int
  )

  /**
    * Remote source client configuration
    * @param retryDelay
    *   SSE client retry delay
    * @param maxBatchSize
    *   the maximum batching size, corresponding to the maximum number of documents uploaded on a bulk request. In this
    *   window, duplicated persistence ids are discarded
    * @param maxTimeWindow
    *   the maximum batching duration. In this window, duplicated persistence ids are discarded
    */
  final case class RemoteSourceClientConfig(
      retryDelay: FiniteDuration,
      maxBatchSize: Int,
      maxTimeWindow: FiniteDuration
  )

  object SinkConfig {

    /** Represents the choice of composite sink */
    sealed trait SinkConfig

    /** A sink that only supports querying one resource at once from blazegraph */
    case object Single extends SinkConfig

    /** A sink that supports querying multiple resources at once from blazegraph */
    case object Batch extends SinkConfig

    given ConfigReader[SinkConfig] =
      ConfigReader.fromString {
        case "batch"  => Right(Batch)
        case "single" => Right(Single)
        case value    =>
          Left(CannotConvert(value, SinkConfig.getClass.getSimpleName, s"$value is not one of: [single, batch]"))
      }
  }

  given ConfigReader[CompositeViewsConfig] = deriveReader[CompositeViewsConfig]
}
