package ai.senscience.nexus.delta.elasticsearch.client

import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams.*
import ai.senscience.nexus.delta.kernel.search.Pagination.{FromPagination, SearchAfterPagination}
import ai.senscience.nexus.delta.kernel.search.{Pagination, TimeRange}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.sdk.implicits.{given, *}
import ai.senscience.nexus.delta.sdk.jsonld.IriEncoder
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.{Sort, SortList}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.literal.json
import io.circe.syntax.*
import io.circe.{Encoder, Json, JsonObject}

final case class QueryBuilder private[client] (
    private val body: JsonObject,
    private val queryParams: Map[String, String]
) {

  private val trackTotalHits = "track_total_hits"
  private val searchAfter    = "search_after"

  private given Encoder[Sort] = Encoder.encodeJson.contramap(sort => Json.obj(sort.value := sort.order))

  /**
    * Adds pagination to the current payload
    *
    * @param page
    *   the pagination information
    */
  def withPage(page: Pagination): QueryBuilder =
    page match {
      case FromPagination(from, size)      => copy(body.add("from", from.asJson).add("size", size.asJson))
      case SearchAfterPagination(sa, size) => copy(body.add(searchAfter, sa.asJson).add("size", size.asJson))
    }

  /**
    * Enables or disables the tracking of total hits count
    */
  def withTotalHits(value: Boolean): QueryBuilder =
    copy(body.add(trackTotalHits, value.asJson))

  /**
    * Adds sort to the current payload
    */
  def withSort(sortList: SortList): QueryBuilder =
    if sortList.isEmpty then this
    else copy(body.add("sort", sortList.values.asJson))

  private def versionTerms(version: VersionParams) =
    version.rev.map(term(nxv.rev.prefix, _)) ++
      version.tag.map(term(nxv.tags.prefix, _))

  private def typesTerms(typeParams: TypeParams) = {
    def applyOperator(terms: List[JsonObject], typeOperator: TypeOperator) =
      if terms.isEmpty then Nil
      else
        typeOperator match {
          case TypeOperator.And => List(and(terms*))
          case TypeOperator.Or  => List(or(terms*))
        }

    val (includeTypes, excludeTypes) = typeParams.values.partitionMap { tpe =>
      val t = term(keywords.tpe, tpe.value)
      Either.cond(!tpe.include, t, t)
    }

    applyOperator(includeTypes, typeParams.operator) ->
      applyOperator(excludeTypes, typeParams.operator.negate)
  }

  private def logTerms(log: LogParam)(using BaseUri) = {
    given Encoder[Subject] = IriEncoder.jsonEncoder[Subject]
    log.createdBy.map(term(nxv.createdBy.prefix, _)) ++
      range(nxv.createdAt.prefix, log.createdAt) ++
      log.updatedBy.map(term(nxv.updatedBy.prefix, _)) ++
      range(nxv.updatedAt.prefix, log.updatedAt)
  }

  private def keywordTerms(keywords: KeywordsParam) =
    keywords.value.map { case (key, value) =>
      term(s"_keywords.$key", value)
    }

  /**
    * Filters by the passed ''params''
    */
  def withFilters(params: ResourcesSearchParams, projects: Set[ProjectRef])(using BaseUri): QueryBuilder = {
    val (includeTypes, excludeTypes) = typesTerms(params.types)
    val projectsTerm                 = or(projects.map { project => term("_project", project) }.toSeq*)
    copy(
      body = body.deepMerge(
        queryPayload(
          mustTerms = includeTypes ++
            logTerms(params.log) ++
            versionTerms(params.version) ++
            params.locate.map { l => or(term(keywords.id, l), term(nxv.self.prefix, l)) } ++
            params.id.map(term(keywords.id, _)) ++
            params.q.map(multiMatch) ++
            params.schema.map(term(nxv.constrainedBy.prefix, _)) ++
            params.deprecated.map(term(nxv.deprecated.prefix, _)) ++
            keywordTerms(params.keywords) ++
            List(projectsTerm),
          mustNotTerms = excludeTypes,
          withScore = params.q.isDefined
        )
      )
    )
  }

  private def or(terms: JsonObject*) =
    JsonObject("bool" := Json.obj("should" := terms))

  private def and(terms: JsonObject*) =
    JsonObject("bool" := Json.obj("must" := terms))

  private def queryPayload(
      mustTerms: List[JsonObject],
      mustNotTerms: List[JsonObject],
      withScore: Boolean
  ): JsonObject = {
    val eval = if withScore then "must" else "filter"
    JsonObject(
      "query" ->
        Json.obj(
          "bool" -> Json.obj(eval := mustTerms).addIfNonEmpty("must_not", mustNotTerms)
        )
    )
  }

  private def range(k: String, timeRange: TimeRange): Option[JsonObject] = {
    import TimeRange.*
    def range(value: Json) = Some(JsonObject("range" -> Json.obj(k -> value)))
    timeRange match {
      case Anytime             => None
      case Before(value)       => range(Json.obj("lt" := value))
      case After(value)        => range(Json.obj("gt" := value))
      case Between(start, end) => range(Json.obj("gt" := start, "lt" := end))
    }
  }

  private def term[A: Encoder](k: String, value: A): JsonObject =
    JsonObject("term" -> Json.obj(k := value))

  /**
    * Defines a multi-match query. If the input [[q]] is an absolute IRI, then the `path_hierarchy` analyzer is used in
    * order to not split the IRI into tokens that are not meaningful.
    */
  private def multiMatch(q: String): JsonObject = {
    val iri      = Iri.reference(q).toOption
    val payload  = JsonObject(
      "multi_match" -> Json.obj(
        "query"  := iri.map(_.toString).getOrElse(q),
        "fields" := json"""[ "*", "*.fulltext", "_tags", "_original_source", "_uuid" ]"""
      )
    )
    val analyzer = JsonObject(
      "multi_match" -> Json.obj("analyzer" := "path_hierarchy")
    )

    iri match {
      case Some(_) => payload.deepMerge(analyzer)
      case None    => payload
    }
  }

  def aggregation(bucketSize: Int): QueryBuilder = {
    val aggregations =
      JsonObject(
        "aggs" := Json.obj(
          termAggregation("projects", "_project", bucketSize),
          termAggregation("types", "@type", bucketSize)
        ),
        "size" := 0
      )

    copy(body = body.deepMerge(aggregations))
  }

  private def termAggregation(name: String, fieldName: String, bucketSize: Int) =
    name -> Json.obj("terms" -> Json.obj("field" := fieldName, "size" := bucketSize))

  def build: ElasticSearchRequest = ElasticSearchRequest(body, queryParams)
}

object QueryBuilder {

  /**
    * An empty [[QueryBuilder]]
    */
  val empty: QueryBuilder = QueryBuilder(JsonObject.empty, Map.empty)

  def unsafe(jsonObject: JsonObject): QueryBuilder = QueryBuilder.unsafe(jsonObject, Map.empty)

  def unsafe(jsonObject: JsonObject, queryParams: Map[String, String]): QueryBuilder =
    QueryBuilder(jsonObject, queryParams)

  /**
    * A [[QueryBuilder]] using the filter ''params''.
    */
  def apply(params: ResourcesSearchParams, projects: Set[ProjectRef])(using BaseUri): QueryBuilder =
    apply(params, projects, Map.empty)

  def apply(params: ResourcesSearchParams, projects: Set[ProjectRef], queryParams: Map[String, String])(using
      BaseUri
  ): QueryBuilder =
    QueryBuilder(JsonObject.empty, queryParams).withFilters(params, projects)
}
