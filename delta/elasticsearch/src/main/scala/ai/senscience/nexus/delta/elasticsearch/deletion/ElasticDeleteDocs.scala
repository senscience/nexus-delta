package ai.senscience.nexus.delta.elasticsearch.deletion

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, IndexLabel}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import io.circe.JsonObject
import io.circe.literal.json

object ElasticDeleteDocs {

  private[deletion] def searchByProject(project: ProjectRef) =
    JsonObject("query" -> json"""{"term": {"_project": $project} }""")

  def inIndices(client: ElasticSearchClient, project: ProjectRef, indices: NonEmptyList[IndexLabel]): IO[Unit] = {
    val search = searchByProject(project)
    indices.parUnorderedTraverse { index =>
      client.deleteByQuery(search, index)
    }.void
  }

}
