package ai.senscience.nexus.delta.elasticsearch.query

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Hits, QueryBuilder}
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.elasticsearch.indexing.mainProjectTargetAlias
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.{AggregationResult, SearchResults, SortList}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json, JsonObject}
import org.http4s.Query

/**
  * Allow to list resources from the main Elasticsearch index
  */
trait MainIndexQuery {

  /**
    * Query the main index with the provided query limiting the search to the given project
    * @param project
    *   the project the query targets
    * @param query
    *   the query to execute
    * @param qp
    *   the extra query parameters for the elasticsearch index
    */
  def search(project: ProjectRef, query: JsonObject, qp: Query): IO[Json]

  /**
    * Retrieves a list of resources from the provided search request on the set of projects
    */
  def list(request: MainIndexRequest, projects: Set[ProjectRef]): IO[SearchResults[JsonObject]]

  /**
    * Retrieves aggregations for the provided search request on the set of projects
    */
  def aggregate(request: MainIndexRequest, projects: Set[ProjectRef]): IO[AggregationResult]
}

object MainIndexQuery {

  private val excludeOriginalSource = "_source_excludes" -> "_original_source"

  def apply(
      client: ElasticSearchClient,
      config: MainIndexConfig
  )(implicit baseUri: BaseUri): MainIndexQuery = new MainIndexQuery {

    override def search(project: ProjectRef, query: JsonObject, qp: Query): IO[Json] = {
      val index = mainProjectTargetAlias(config.index, project)
      client.search(query, Set(index.value), qp)(SortList.empty)
    }

    override def list(request: MainIndexRequest, projects: Set[ProjectRef]): IO[SearchResults[JsonObject]] = {
      val query =
        QueryBuilder(request.params, projects).withPage(request.pagination).withTotalHits(true).withSort(request.sort)
      client.search(query, Set(config.index.value), Query.fromPairs(excludeOriginalSource))
    }

    override def aggregate(request: MainIndexRequest, projects: Set[ProjectRef]): IO[AggregationResult] = {
      val query = QueryBuilder(request.params, projects).aggregation(config.bucketSize)
      client.searchAs[AggregationResult](query, config.index.value, Query.empty)
    }
  }

  implicit val aggregationDecoder: Decoder[AggregationResult] =
    Decoder.decodeJsonObject.emap { result =>
      result.asJson.hcursor
        .downField("aggregations")
        .focus
        .flatMap(_.asObject) match {
        case Some(aggs) => Right(AggregationResult(Hits.fetchTotal(result), aggs))
        case None       => Left("The response did not contain a valid 'aggregations' field.")
      }
    }
}
