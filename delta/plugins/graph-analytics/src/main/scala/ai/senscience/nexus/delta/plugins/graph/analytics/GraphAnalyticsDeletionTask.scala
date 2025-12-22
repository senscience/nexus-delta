package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.plugins.graph.analytics.GraphAnalytics.index
import ai.senscience.nexus.delta.plugins.graph.analytics.config.GraphAnalyticsConfig
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef}
import cats.effect.IO

class GraphAnalyticsDeletionTask(client: ElasticSearchClient, config: GraphAnalyticsConfig)
    extends ProjectDeletionTask {

  private val reportStage =
    ProjectDeletionReport.Stage(
      "graph-analytics",
      "The graph analytics have been successfully removed from Elasticsearch."
    )

  override def apply(project: ProjectRef)(using Identity.Subject): IO[ProjectDeletionReport.Stage] =
    client.deleteIndex(index(config.prefix, project)).as(reportStage)

}
