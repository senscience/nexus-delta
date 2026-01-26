package ai.senscience.nexus.delta.elasticsearch.metrics

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchAction.Index
import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient.given
import ai.senscience.nexus.delta.elasticsearch.client.{BulkResponse, ElasticSearchClient, ElasticSearchRequest, Refresh}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.metrics.EventMetric.ProjectScopedMetric
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import io.circe.literal.json
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Json, JsonObject}

trait FetchHistory {
  def history(project: ProjectRef, id: Iri): IO[SearchResults[JsonObject]]
}

trait EventMetrics extends FetchHistory {

  def init: IO[Unit]

  def destroy: IO[Unit]

  def index(bulk: Vector[ProjectScopedMetric]): IO[BulkResponse]

  def deleteByProject(project: ProjectRef): IO[Unit]

  def deleteByResource(project: ProjectRef, id: Iri): IO[Unit]
}

object EventMetrics {

  def apply(client: ElasticSearchClient, metricIndex: MetricsIndexDef): EventMetrics = new EventMetrics {

    private val index = metricIndex.name

    override def init: IO[Unit] = client.createIndex(index, metricIndex.indexDef).void

    override def destroy: IO[Unit] = client.deleteIndex(index).void

    override def index(bulk: Vector[ProjectScopedMetric]): IO[BulkResponse] = {
      val actions = bulk.map { metric =>
        Index(index, metric.eventId, None, metric.asJson)
      }
      client.bulk(actions, Refresh.False)
    }

    override def deleteByProject(project: ProjectRef): IO[Unit] = {
      val deleteByProject = ElasticSearchRequest(
        "query" -> json"""{"term": {"project": ${project.asJson} } }"""
      )
      client.deleteByQuery(deleteByProject, index)
    }

    override def deleteByResource(project: ProjectRef, id: Iri): IO[Unit] = {
      val deleteByResource = ElasticSearchRequest(
        "query" ->
          json"""{
                   "bool": {
                     "must": [
                       { "term": { "project": ${project.asJson} } },
                       { "term": { "@id": ${id.asJson} } }
                     ]
                   }
                 }"""
      )
      client.deleteByQuery(deleteByResource, index)
    }

    override def history(project: ProjectRef, id: Iri): IO[SearchResults[JsonObject]] = {
      val request = ElasticSearchRequest(
        JsonObject(
          "query" := json"""{
                  "bool": {
                    "must": [
                      { "term": { "project": ${project.asJson} } },
                      { "term": { "@id": ${id.asJson} } }
                    ]
                  }
                }""",
          "size"  := 2000,
          "from"  := 0,
          "sort"  := Json.arr(json"""{ "rev": { "order" : "asc" } }""")
        )
      )
      client.searchAs[SearchResults[JsonObject]](request, Set(index.value))
    }
  }

}
