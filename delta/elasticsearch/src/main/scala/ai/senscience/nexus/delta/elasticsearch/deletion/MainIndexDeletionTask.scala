package ai.senscience.nexus.delta.elasticsearch.deletion

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, IndexLabel}
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef}
import cats.data.NonEmptyList
import cats.effect.IO

final class MainIndexDeletionTask(client: ElasticSearchClient, mainIndex: IndexLabel) extends ProjectDeletionTask {

  private val reportStage =
    ProjectDeletionReport.Stage("main-index", "The project has been successfully removed from the main index.")

  override def apply(project: ProjectRef)(using Identity.Subject): IO[ProjectDeletionReport.Stage] =
    ElasticDeleteDocs.inIndices(client, project, NonEmptyList.one(mainIndex)).as(reportStage)
}
