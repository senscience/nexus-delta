package ai.senscience.nexus.delta.elasticsearch.deletion

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, IndexLabel}
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef}
import cats.effect.IO
import io.circe.parser.parse

final class MainIndexDeletionTask(client: ElasticSearchClient, mainIndex: IndexLabel) extends ProjectDeletionTask {

  private val reportStage =
    ProjectDeletionReport.Stage("default-index", "The project has been successfully removed from the default index.")

  override def apply(project: ProjectRef)(using Identity.Subject): IO[ProjectDeletionReport.Stage] = {
    searchByProject(project).flatMap { search =>
      client
        .deleteByQuery(search, mainIndex)
        .as(reportStage)
    }
  }

  private[deletion] def searchByProject(project: ProjectRef) =
    IO.fromEither {
      parse(s"""{"query": {"term": {"_project": "$project"} } }""").flatMap(
        _.asObject.toRight(new IllegalStateException("Failed to convert to json object the search query."))
      )
    }
}
