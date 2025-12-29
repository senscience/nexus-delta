package ai.senscience.nexus.delta.elasticsearch.deletion

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, IndexLabel}
import ai.senscience.nexus.delta.elasticsearch.configured.ConfiguredIndexingConfig
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef}
import cats.data.NonEmptyList
import cats.effect.IO

final class ConfiguredIndexDeletionTask(client: ElasticSearchClient, indices: NonEmptyList[IndexLabel])
    extends ProjectDeletionTask {

  private val reportStage =
    ProjectDeletionReport.Stage(
      "configured-index",
      "The project has been successfully removed from the configured index."
    )

  override def apply(project: ProjectRef)(using Identity.Subject): IO[ProjectDeletionReport.Stage] =
    ElasticDeleteDocs.inIndices(client, project, indices).as(reportStage)
}

object ConfiguredIndexDeletionTask {
  def apply(client: ElasticSearchClient, config: ConfiguredIndexingConfig): Option[ConfiguredIndexDeletionTask] =
    config match {
      case ConfiguredIndexingConfig.Disabled                                   => None
      case ConfiguredIndexingConfig.Enabled(prefix, maxRefreshPeriod, indices) =>
        Some(
          new ConfiguredIndexDeletionTask(
            client,
            indices.map(_.prefixedIndex(prefix))
          )
        )
    }
}
