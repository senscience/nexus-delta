package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.plugins.graph.analytics.config.GraphAnalyticsConfig.TermAggregationsConfig
import cats.effect.IO
import io.circe.JsonObject

package object indexing {

  private val logger                            = Logger[GraphAnalytics]
  private given loader: ClasspathResourceLoader =
    ClasspathResourceLoader.withContext(classOf[GraphAnalyticsPluginModule])

  val updateRelationshipsScriptId = "updateRelationships"

  val scriptContent: IO[String] =
    loader
      .contentOf("elasticsearch/update_relationships_script.painless")
      .onError { case e =>
        logger.warn(e)("ElasticSearch script 'update_relationships_script.painless' template not found")
      }

  val graphAnalyticsIndexDef: IO[ElasticsearchIndexDef] =
    ElasticsearchIndexDef
      .load(
        "elasticsearch/mappings.json",
        None
      )
      .onError { case e => logger.warn(e)("ElasticSearch mapping 'mappings.json' template not found") }

  def propertiesAggQuery(config: TermAggregationsConfig): IO[JsonObject] = loader
    .jsonObjectContentOf(
      "elasticsearch/paths-properties-aggregations.json",
      "shard_size" -> config.shardSize,
      "size"       -> config.size,
      "type"       -> "{{type}}"
    )
    .onError { err =>
      logger.error(err)("ElasticSearch 'paths-properties-aggregations.json' template not found")
    }

  def relationshipsAggQuery(config: TermAggregationsConfig): IO[JsonObject] =
    loader
      .jsonObjectContentOf(
        "elasticsearch/paths-relationships-aggregations.json",
        "shard_size" -> config.shardSize,
        "size"       -> config.size
      )
      .onError { err =>
        logger.error(err)("ElasticSearch 'paths-relationships-aggregations.json' template not found")
      }
}
