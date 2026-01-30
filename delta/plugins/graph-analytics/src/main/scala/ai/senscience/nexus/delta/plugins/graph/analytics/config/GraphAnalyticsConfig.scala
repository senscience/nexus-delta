package ai.senscience.nexus.delta.plugins.graph.analytics.config

import ai.senscience.nexus.delta.plugins.graph.analytics.config.GraphAnalyticsConfig.TermAggregationsConfig
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import pureconfig.ConfigReader
import pureconfig.error.FailureReason
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for the graph analytics plugin.
  *
  * @param batch
  *   a configuration definition how often we want to push to Elasticsearch
  * @param prefix
  *   prefix for indices
  * @param termAggregations
  *   the term aggregations query configuration
  * @param indexingEnabled
  *   if true, disables graph analytics indexing
  */
final case class GraphAnalyticsConfig(
    batch: BatchConfig,
    prefix: String,
    termAggregations: TermAggregationsConfig,
    indexingEnabled: Boolean
)

object GraphAnalyticsConfig {

  /**
    * Configuration for term aggregation queries
    * @param size
    *   the global number of terms returned by the aggregation. The term aggregation is requested to each shard and once
    *   all the shards responded, the coordinating node will then reduce them to a final result which will be based on
    *   this size parameter The higher the requested size is, the more accurate the results will be, but also, the more
    *   expensive it will be to compute the final results
    * @param shardSize
    *   the number of terms the coordinating node returns from each shard. This value must be higher than ''size''
    */
  final case class TermAggregationsConfig(size: Int, shardSize: Int)

  private given ConfigReader[TermAggregationsConfig] = deriveReader[TermAggregationsConfig]

  given ConfigReader[GraphAnalyticsConfig] =
    deriveReader[GraphAnalyticsConfig].emap { c =>
      validateAggregations(c.termAggregations).map(_ => c)
    }

  private def validateAggregations(cfg: TermAggregationsConfig) =
    Either.cond(
      cfg.shardSize > cfg.size,
      (),
      failure("'shard-size' must be greater than 'size' (recommended shard-size ~ 1.5 size)")
    )

  private def failure(reason: String): FailureReason =
    new FailureReason {
      override def description: String = reason
    }
}
