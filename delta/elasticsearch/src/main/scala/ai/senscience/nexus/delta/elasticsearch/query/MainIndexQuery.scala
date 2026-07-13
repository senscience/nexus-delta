package ai.senscience.nexus.delta.elasticsearch.query

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, ElasticSearchRequest, Hits, QueryBuilder}
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.sdk.indexing.MetadataFields
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.{AggregationResult, SearchResults, SortList}
import ai.senscience.nexus.delta.sdk.syntax.surround
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import cats.syntax.functor.*
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json, JsonObject}
import org.typelevel.otel4s.trace.Tracer

/**
  * Allow to list resources from the main Elasticsearch index
  */
trait MainIndexQuery {

  /**
    * Query the main index with the provided query limiting the search to the given project
    * @param project
    *   the project the query targets
    * @param request
    *   the request to execute
    */
  def search(project: ProjectRef, request: ElasticSearchRequest): IO[Json]

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

  private val excludeOriginalSource = Map("_source_excludes" -> MetadataFields.indexField("_original_source"))

  /** Rewrites sort fields to their physical location in the main index (system metadata nested under `_nexus`). */
  private def indexSort(sort: SortList): SortList =
    SortList(sort.values.map(s => s.copy(value = MetadataFields.indexField(s.value))))

  /** Hoists the `_nexus` system metadata back to the root so the listing keeps its flat, historical shape. */
  private def flattenMetadata(source: JsonObject): JsonObject = {
    val nexus = source(MetadataFields.umbrella).flatMap(_.asObject).getOrElse(JsonObject.empty)
    nexus.toIterable.foldLeft(source.remove(MetadataFields.umbrella)) { case (acc, (key, value)) =>
      acc.add(key, value)
    }
  }

  def apply(
      client: ElasticSearchClient,
      config: MainIndexConfig
  )(using BaseUri, Tracer[IO]): MainIndexQuery = new MainIndexQuery {

    override def search(project: ProjectRef, request: ElasticSearchRequest): IO[Json] = {
      client.search(FilterByProject(project, request), Set(config.index.value))
    }.surround("mainUserQuery")

    override def list(request: MainIndexRequest, projects: Set[ProjectRef]): IO[SearchResults[JsonObject]] = {
      val query =
        QueryBuilder(request.params, projects, excludeOriginalSource)
          .withPage(request.pagination)
          .withTotalHits(true)
          .withSort(indexSort(request.sort))
      client.search(query, Set(config.index.value)).map(_.map(flattenMetadata))
    }.surround("mainListQuery")

    override def aggregate(request: MainIndexRequest, projects: Set[ProjectRef]): IO[AggregationResult] = {
      val query = QueryBuilder(request.params, projects).aggregation(config.bucketSize)
      client.searchAs[AggregationResult](query, config.index.value)
    }.surround("mainAggregate")
  }

  given Decoder[AggregationResult] =
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
