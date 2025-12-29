package ai.senscience.nexus.delta.elasticsearch.deletion

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, IndexLabel}
import ai.senscience.nexus.delta.elasticsearch.query.SearchByProject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*

object ElasticDeleteDocs {

  def inIndices(client: ElasticSearchClient, project: ProjectRef, indices: NonEmptyList[IndexLabel]): IO[Unit] = {
    val searchByProject = SearchByProject(project)
    indices.parUnorderedTraverse { index =>
      client.deleteByQuery(searchByProject, index)
    }.void
  }

}
