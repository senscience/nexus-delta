package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, ElasticSearchRequest, IndexLabel}
import ai.senscience.nexus.delta.plugins.storage.files.nxvFile
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageStatEntry
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import io.circe.literal.*

trait StoragesStatistics {

  /**
    * Retrieve the current statistics for a given storage in the given project
    */
  def get(idSegment: IdSegment, project: ProjectRef): IO[StorageStatEntry]

}

object StoragesStatistics {

  /**
    * @param client
    *   the Elasticsearch client
    * @param fetchStorageId
    *   the function to fetch the storage ID
    * @param index
    *   the index containing the event metrics
    * @return
    *   StorageStatistics instance
    */
  def apply(
      client: ElasticSearchClient,
      fetchStorageId: (IdSegment, ProjectRef) => IO[Iri],
      index: IndexLabel
  ): StoragesStatistics = {
    val search = (request: ElasticSearchRequest) => client.search(request, Set(index.value))

    (idSegment: IdSegment, project: ProjectRef) => {
      for {
        storageId <- fetchStorageId(idSegment, project)
        request    = storageStatisticsQuery(project, storageId)
        result    <- search(request)
        stats     <- IO.fromEither(result.as[StorageStatEntry])
      } yield stats
    }
  }

  /**
    * @param projectRef
    *   the project on which the statistics should be computed
    * @param storageId
    *   the ID of the storage on which the statistics should be computed
    * @return
    *   a query for the total number of files and the total size of a storage in a given project
    */
  private def storageStatisticsQuery(projectRef: ProjectRef, storageId: Iri): ElasticSearchRequest =
    ElasticSearchRequest(
      "query" ->
        json"""
            {
            "bool": {
              "filter": [
                { "term": { "@type": $nxvFile } },
                { "term": { "project": $projectRef } },
                { "term": { "storage": $storageId } }
              ]
            }
          }
         """,
      "aggs"  -> json"""{
          "storageSize": { "sum": { "field": "bytes" } },
          "filesCount": { "sum": { "field": "newFileWritten" } }
        }""",
      "size"  -> json"0"
    )

}
