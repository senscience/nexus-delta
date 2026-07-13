package ai.senscience.nexus.delta.elasticsearch.deletion

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, ElasticSearchRequest, IndexLabel}
import ai.senscience.nexus.delta.sdk.indexing.MetadataFields
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.KeyOps
import io.circe.{Json, JsonObject}

object ElasticDeleteDocs {

  private[deletion] def searchByProject(project: ProjectRef) =
    JsonObject("query" -> Json.obj("term" -> Json.obj(MetadataFields.indexField("_project") := project)))

  def inIndices(client: ElasticSearchClient, project: ProjectRef, indices: NonEmptyList[IndexLabel]): IO[Unit] = {
    val search = ElasticSearchRequest(searchByProject(project))
    indices.parUnorderedTraverse { index =>
      client.deleteByQuery(search, index)
    }.void
  }

}
