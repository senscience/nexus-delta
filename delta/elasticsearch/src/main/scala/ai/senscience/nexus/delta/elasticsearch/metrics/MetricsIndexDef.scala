package ai.senscience.nexus.delta.elasticsearch.metrics

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.IO

/**
  * Configuration for the index for event metrics
  */
final case class MetricsIndexDef(name: IndexLabel, indexDef: ElasticsearchIndexDef)

object MetricsIndexDef {

  def apply(prefix: String, serverless: Boolean = false)(using loader: ClasspathResourceLoader): IO[MetricsIndexDef] =
    ElasticsearchIndexDef
      .fromClasspath(
        "metrics/metrics-mapping.json",
        Some("metrics/metrics-settings.json"),
        "serverless" -> serverless
      )
      .map { d => MetricsIndexDef(IndexLabel.unsafe(s"${prefix}_project_metrics"), d) }

}
