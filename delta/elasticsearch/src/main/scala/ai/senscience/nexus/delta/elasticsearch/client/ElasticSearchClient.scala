package ai.senscience.nexus.delta.elasticsearch.client

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient.*
import ai.senscience.nexus.delta.elasticsearch.config.ElasticSearchViewsConfig.OpentelemetryConfig
import ai.senscience.nexus.delta.elasticsearch.query.ElasticSearchClientError.*
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription.ResolvedServiceDescription
import ai.senscience.nexus.delta.kernel.http.circe.*
import ai.senscience.nexus.delta.kernel.http.circe.CirceEntityDecoder.*
import ai.senscience.nexus.delta.kernel.http.circe.CirceEntityEncoder.*
import ai.senscience.nexus.delta.kernel.http.client.middleware.BasicAuth
import ai.senscience.nexus.delta.kernel.utils.UrlUtils
import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.{ScoredResultEntry, UnscoredResultEntry}
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.{ScoredSearchResults, UnscoredSearchResults}
import ai.senscience.nexus.delta.sdk.model.search.{ResultEntry, SearchResults, SortList}
import ai.senscience.nexus.delta.sdk.otel.{OtelClient, SpanDef}
import ai.senscience.nexus.delta.sdk.syntax.*
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.circe.*
import io.circe.literal.*
import io.circe.syntax.*
import org.http4s.Method.*
import org.http4s.client.Client
import org.http4s.client.dsl.io.*
import org.http4s.client.middleware.GZip
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.`Content-Type`
import org.http4s.{BasicCredentials, EntityEncoder, MediaType, Query, Status, Uri}
import org.typelevel.otel4s.{Attribute, AttributeKey, Attributes}
import org.typelevel.otel4s.trace.Tracer

import java.net.{ConnectException, UnknownHostException}
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

/**
  * A client that provides some of the functionality of the elasticsearch API.
  */
final class ElasticSearchClient(client: Client[IO], endpoint: Uri, maxIndexPathLength: Int, otel: OpentelemetryConfig)(
    using Tracer[IO]
) {

  private val serviceName            = "elasticsearch"
  private val scriptPath             = "_scripts"
  private val docPath                = "_doc"
  private val allIndexPath           = "_all"
  private val aliasPath              = "_aliases"
  private val bulkPath               = "_bulk"
  private val refreshPath            = "_refresh"
  private val indexTemplate          = "_index_template"
  private val tasksPath              = "_tasks"
  private val waitForCompletion      = "wait_for_completion"
  private val refreshParam           = "refresh"
  private val ignoreUnavailable      = "ignore_unavailable"
  private val allowNoIndices         = "allow_no_indices"
  private val deleteByQueryPath      = "_delete_by_query"
  private val updateByQueryPath      = "_update_by_query"
  private val countPath              = "_count"
  private val searchPath             = "_search"
  private val source                 = "_source"
  private val mapping                = "_mapping"
  private val pit                    = "_pit"
  private val newLine                = System.lineSeparator()
  private val `application/x-ndjson` =
    new MediaType("application", "x-ndjson", compressible = true, fileExtensions = List("json"))
  private val defaultQuery           = Map(ignoreUnavailable -> "true", allowNoIndices -> "true")
  private val defaultUpdateByQuery   = defaultQuery + (waitForCompletion -> "false")
  private val defaultDeleteByQuery   = defaultQuery + (waitForCompletion -> "true")

  private val indexKey = AttributeKey[String]("nexus.elasticsearch.index")

  private def withIndex(index: String): Attribute[String] = Attribute(indexKey, index)

  private def withIndex(index: IndexLabel): Attribute[String] = withIndex(index.value)

  private val queryKey = AttributeKey[String]("nexus.elasticsearch.query")

  private def searchQuery(query: JsonObject) = {
    val attributes = Attributes.newBuilder
    attributes ++= Option.when(otel.captureQueries)(Attribute(queryKey, query.toJson.noSpaces))
    attributes += read
    SpanDef(s"<string:index>/$searchPath", attributes.result())
  }

  private val accessKey = AttributeKey[String]("nexus.elasticsearch.access")
  private val read      = Attribute(accessKey, "read")
  private val write     = Attribute(accessKey, "write")

  /**
    * Fetches the service description information (name and version)
    */
  def serviceDescription: IO[ServiceDescription] =
    client
      .expect[ResolvedServiceDescription](endpoint)
      .timeout(1.second)
      .recover(_ => ServiceDescription.unresolved(serviceName))

  /**
    * Verifies if an index exists, recovering gracefully when the index does not exists.
    *
    * @param index
    *   the index to verify
    * @return
    *   ''true'' when the index exists and ''false'' when it doesn't, wrapped in an IO
    */
  def existsIndex(index: IndexLabel): IO[Boolean] = {
    val spanDef = SpanDef("<string:index>", withIndex(index), read)
    OtelClient(client, spanDef).status(HEAD(endpoint / index.value)).flatMap {
      case Status.Ok       => IO.pure(true)
      case Status.NotFound => IO.pure(false)
      case status          => IO.raiseError(ElasticsearchActionError(status, "exists"))
    }
  }

  /**
    * Attempts to create an index recovering gracefully when the index already exists.
    *
    * @param index
    *   the index
    * @param payload
    *   the payload to attach to the index when it does not exist
    * @return
    *   ''true'' when the index has been created and ''false'' when it already existed, wrapped in an IO
    */
  def createIndex(index: IndexLabel, payload: JsonObject = JsonObject.empty): IO[Boolean] =
    existsIndex(index).flatMap {
      case false =>
        val spanDef = SpanDef("<string:index>", withIndex(index), write)
        val request = PUT(payload, endpoint / index.value)
        OtelClient(client, spanDef).expectOr[Json](request)(ElasticsearchCreateIndexError(_)).as(true)
      case true  => IO.pure(false)
    }

  /**
    * Attempts to create an index recovering gracefully when the index already exists.
    *
    * @param index
    *   the index
    * @param mappings
    *   the optional mappings section of the index payload
    * @param settings
    *   the optional settings section of the index payload
    * @return
    *   ''true'' when the index has been created and ''false'' when it already existed, wrapped in an IO
    */
  def createIndex(index: IndexLabel, mappings: Option[JsonObject], settings: Option[JsonObject]): IO[Boolean] =
    createIndex(index, JsonObject.empty.addIfExists("mappings", mappings).addIfExists("settings", settings))

  /**
    * Attempts to create an index template
    *
    * @param name
    *   the template name
    * @param template
    *   the template payload
    * @return
    *   ''true'' when the index template has been created
    */
  def createIndexTemplate(name: String, template: JsonObject): IO[Boolean] = {
    val spanDef = SpanDef(s"$indexTemplate/<string:template>", write)
    OtelClient(client, spanDef).status(PUT(template, endpoint / indexTemplate / name)).flatMap {
      case Status.Ok       => IO.pure(true)
      case Status.NotFound => IO.pure(false)
      case status          => IO.raiseError(ElasticsearchActionError(status, "createTemplate"))
    }
  }

  /**
    * Attempts to delete an index recovering gracefully when the index is not found.
    *
    * @param index
    *   the index
    * @return
    *   ''true'' when the index has been deleted and ''false'' when it didn't exist, wrapped in an IO
    */
  def deleteIndex(index: IndexLabel): IO[Boolean] = {
    val spanDef = SpanDef("<string:index>", withIndex(index), write)
    OtelClient(client, spanDef).status(DELETE(endpoint / index.value)).flatMap {
      case Status.Ok       => IO.pure(true)
      case Status.NotFound => IO.pure(false)
      case status          => IO.raiseError(ElasticsearchActionError(status, "deleteIndex"))
    }
  }

  /**
    * Creates or replaces a new document inside the ''index'' with the provided ''payload''
    *
    * @param index
    *   the index to use
    * @param id
    *   the id of the document to update
    * @param payload
    *   the document's payload
    */
  def replace(
      index: IndexLabel,
      id: String,
      payload: JsonObject
  ): IO[Unit] = {
    val spanDef = SpanDef(s"<string:index>/$docPath/<string:id>", withIndex(index), write)
    OtelClient(client, spanDef).successful(PUT(payload, endpoint / index.value / docPath / UrlUtils.encodeUri(id))).void
  }

  /**
    * Creates a bulk update with the operations defined on the provided ''ops'' argument.
    *
    * @param actions
    *   the list of operations to be included in the bulk update
    * @param refresh
    *   the value for the `refresh` Elasticsearch parameter
    */
  def bulk(actions: Seq[ElasticSearchAction], refresh: Refresh = Refresh.False): IO[BulkResponse] = {
    if actions.isEmpty then IO.pure(BulkResponse.Success)
    else {
      val payload      = actions.map(_.payload).mkString("", newLine, newLine)
      val bulkEndpoint = (endpoint / bulkPath).withQueryParam(refreshParam, refresh.value)
      val request      = POST(payload, bulkEndpoint, `Content-Type`(`application/x-ndjson`))(EntityEncoder.stringEncoder)
      val spanDef      = SpanDef(bulkPath, write)
      OtelClient(client, spanDef).expectOr[BulkResponse](request)(ElasticsearchWriteError(_)).flatTap {
        case BulkResponse.Success          => logger.debug("All operations in the bulk succeeded.")
        case BulkResponse.MixedOutcomes(_) =>
          logger.error("Some operations in the bulk failed, please check the indexing failures to find the reason(s).")
      }
    }
  }

  /**
    * Creates a script on Elasticsearch with the passed ''id'' and ''content''
    */
  def createScript(id: String, content: String): IO[Unit] = {
    val payload = Json.obj("script" -> Json.obj("lang" -> "painless".asJson, "source" -> content.asJson))
    val spanDef = SpanDef(scriptPath, write)
    val request = PUT(payload, endpoint / scriptPath / UrlUtils.encodeUri(id))
    OtelClient(client, spanDef).expectOr[Json](request)(ScriptCreationDismissed(_)).void
  }

  /**
    * Runs an update by query with the passed ''query'' and ''indices''. The query is run as a task and the task is
    * requested until it finished
    *
    * @param query
    *   the search query
    * @param indices
    *   the indices targeted by the update query
    */
  def updateByQuery(query: JsonObject, indices: Set[String]): IO[Unit] = {
    val (indexPath, q)       = indexPathAndQuery(indices, QueryBuilder(query))
    val updateEndpoint       = (endpoint / indexPath / updateByQueryPath).withQueryParams(defaultUpdateByQuery)
    val updateByQuerySpanDef = SpanDef(s"<string:index>/$updateByQueryPath", write)
    val taskSpanDef          = SpanDef(s"$tasksPath/<string:task>", write)
    val request              = POST(q.build, updateEndpoint)
    for {
      response <- OtelClient(client, updateByQuerySpanDef).expect[UpdateByQueryResponse](request)
      taskReq   = GET((endpoint / tasksPath / response.task).withQueryParam(waitForCompletion, "true"))
      _        <- OtelClient(client, taskSpanDef).expect[Json](taskReq)
    } yield ()
  }

  /**
    * Runs an delete by query with the passed ''query'' and ''index''. The query is run as a task and the task is
    * requested until it finished
    *
    * @param query
    *   the search query
    * @param index
    *   the index targeted by the delete query
    */
  def deleteByQuery(query: JsonObject, index: IndexLabel): IO[Unit] = {
    val deleteEndpoint = (endpoint / index.value / deleteByQueryPath).withQueryParams(defaultDeleteByQuery)
    val spanDef        = SpanDef(s"<string:index>/$deleteByQueryPath", withIndex(index), write)
    val req            = POST(query, deleteEndpoint)
    OtelClient(client, spanDef).expect[Json](req).void
  }

  /**
    * Get the source of the Elasticsearch document
    * @param index
    *   the index to look in
    * @param id
    *   the identifier of the document
    */
  def getSource[R: Decoder](index: IndexLabel, id: String): IO[Option[R]] = {
    val sourceEndpoint = endpoint / index.value / source / id
    val spanDef        = SpanDef(s"<string:index>/$source/<string:id>", withIndex(index), read)
    OtelClient(client, spanDef).expectOption[R](GET(sourceEndpoint))
  }

  /**
    * Returns the number of document in a given index
    * @param index
    *   the index to use
    */
  def count(index: String): IO[Long] = {
    val spanDef = SpanDef(s"<string:index>/$countPath", withIndex(index), read)
    OtelClient(client, spanDef).expect[Count](GET(endpoint / index / countPath)).map(_.value)
  }

  /**
    * Search for the provided ''query'' inside the ''index'' returning a parsed result as a [[SearchResults]].
    *
    * @param query
    *   the search query
    * @param indices
    *   the indices to use on search (if empty, searches in all the indices)
    * @param qp
    *   the optional query parameters
    */
  def search(
      query: QueryBuilder,
      indices: Set[String],
      qp: Query
  ): IO[SearchResults[JsonObject]] =
    searchAs[SearchResults[JsonObject]](query, indices, qp)(SearchResults.empty)

  /**
    * Search for the provided ''query'' inside the ''indices''
    *
    * @param query
    *   the initial search query
    * @param indices
    *   the indices to use on search (if empty, searches in all the indices)
    * @param qp
    *   the optional query parameters
    * @param sort
    *   the sorting criteria
    */
  def search(
      query: JsonObject,
      indices: Set[String],
      qp: Query
  )(
      sort: SortList = SortList.empty
  ): IO[Json] =
    if indices.isEmpty then IO.pure(emptyResults)
    else {
      val (indexPath, q) = indexPathAndQuery(indices, QueryBuilder(query))
      val searchEndpoint = (endpoint / indexPath / searchPath).withQueryParams(defaultQuery ++ qp.params)
      val payload        = q.withSort(sort).withTotalHits(true).build
      val spanDef        = searchQuery(query)
      OtelClient(client, spanDef).expectOr[Json](POST(payload, searchEndpoint))(ElasticsearchQueryError(_))
    }

  /**
    * Search for the provided ''query'' inside the ''indices'' returning a parsed result as [[T]].
    *
    * @param query
    *   the search query
    * @param indices
    *   the indices to use on search
    * @param qp
    *   the optional query parameters
    */
  def searchAs[T: Decoder](
      query: QueryBuilder,
      indices: Set[String],
      qp: Query
  )(onEmpty: => T): IO[T] =
    if indices.isEmpty then IO.pure(onEmpty)
    else {
      val (indexPath, q) = indexPathAndQuery(indices, query)
      val searchEndpoint = (endpoint / indexPath / searchPath).withQueryParams(defaultQuery ++ qp.params)
      val queryJson      = q.build
      val spanDef        = searchQuery(queryJson)
      OtelClient(client, spanDef).expect[T](POST(queryJson, searchEndpoint))
    }

  /**
    * Search for the provided ''query'' inside the ''indices'' returning a parsed result as [[T]].
    *
    * @param query
    *   the search query
    * @param index
    *   the index to use on search
    * @param qp
    *   the optional query parameters
    */
  def searchAs[T: Decoder](
      query: QueryBuilder,
      index: String,
      qp: Query
  ): IO[T] = {
    val searchEndpoint = (endpoint / index / searchPath).withQueryParams(defaultQuery ++ qp.params)
    val queryJson      = query.build
    val spanDef        = searchQuery(queryJson)
    OtelClient(client, spanDef).expect[T](POST(queryJson, searchEndpoint))
  }

  /**
    * Refresh the given index
    */
  def refresh(index: IndexLabel): IO[Boolean] = {
    val spanDef = SpanDef(s"<string:index>/$refreshPath", withIndex(index), write)
    OtelClient(client, spanDef).successful(POST(endpoint / index.value / refreshPath))
  }

  /**
    * Obtain the mapping of the given index
    */
  def mapping(index: IndexLabel): IO[Json] = {
    val spanDef = SpanDef(s"<string:index>/$mapping", withIndex(index), read)
    OtelClient(client, spanDef).expect[Json](GET(endpoint / index.value / mapping))
  }

  /**
    * Creates a point-in-time to be used in further searches
    *
    * @see
    *   https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
    * @param index
    *   the target index
    * @param keepAlive
    *   extends the time to live of the corresponding point in time
    */
  def createPointInTime(index: IndexLabel, keepAlive: FiniteDuration): IO[PointInTime] = {
    val pitEndpoint = (endpoint / index.value / pit).withQueryParam("keep_alive", s"${keepAlive.toSeconds}s")
    val spanDef     = SpanDef(s"<string:index>/$pit", withIndex(index), read)
    OtelClient(client, spanDef).expect[PointInTime](POST(pitEndpoint))
  }

  /**
    * Deletes the given point-in-time
    *
    * @see
    *   https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
    */
  def deletePointInTime(pointInTime: PointInTime): IO[Unit] = {
    val spanDef = SpanDef(s"<string:index>/$pit", read)
    OtelClient(client, spanDef).successful(DELETE(pointInTime, endpoint / pit)).void
  }

  def createAlias(indexAlias: IndexAlias): IO[Unit] = {
    val aliasPayload = Json.obj(
      "index"   := indexAlias.index.value,
      "alias"   := indexAlias.alias.value,
      "routing" := indexAlias.routing,
      "filter"  := indexAlias.filter
    )
    aliasAction(Json.obj("add" := aliasPayload))
  }

  def removeAlias(index: IndexLabel, alias: IndexLabel): IO[Unit] = {
    val aliasPayload = Json.obj(
      "index" := index.value,
      "alias" := alias.value
    )
    aliasAction(Json.obj("remove" := aliasPayload))
  }

  private def aliasAction(aliasAction: Json) = {
    val aliasWrap = Json.obj("actions" := Json.arr(aliasAction))
    val spanDef   = SpanDef(aliasPath, write)
    OtelClient(client, spanDef)
      .expectOr[Json](POST(aliasWrap, endpoint / aliasPath)) { r =>
        IO.pure(ElasticsearchActionError(r.status, "createAlias"))
      }
      .void
  }

  private def indexPathAndQuery(indices: Set[String], query: QueryBuilder): (String, QueryBuilder) = {
    val indexPath = indices.mkString(",")
    if indexPath.length < maxIndexPathLength then (indexPath, query)
    else (allIndexPath, query.withIndices(indices))
  }

  given Decoder[ResolvedServiceDescription] =
    Decoder.instance { hc =>
      hc.downField("version").get[String]("number").map(ServiceDescription(serviceName, _))
    }

}

object ElasticSearchClient {

  private val logger = Logger[this.type]

  private def errorHandler(client: Client[IO]): Client[IO] =
    Client { request =>
      client.run(request).adaptError {
        case c: ConnectException     => ElasticSearchConnectError(c)
        case _: UnknownHostException => ElasticSearchUnknownHost
        case t: TimeoutException     => ElasticSearchTimeoutError(t)
      }
    }

  def apply(
      endpoint: Uri,
      credentials: Option[BasicCredentials],
      maxIndexPathLength: Int,
      otel: OpentelemetryConfig
  )(using Tracer[IO]): Resource[IO, ElasticSearchClient] =
    EmberClientBuilder
      .default[IO]
      .withLogger(logger)
      .build
      .map { client =>
        val enrichedClient = errorHandler(
          GZip()(BasicAuth(credentials)(client))
        )
        new ElasticSearchClient(enrichedClient, endpoint, maxIndexPathLength, otel)
      }

  private val emptyResults = json"""{
                                     "hits": {
                                        "hits": [],
                                        "total": {
                                         "relation": "eq",
                                           "value": 0
                                        }
                                     }
                                   }"""

  private def queryResults(json: JsonObject, scored: Boolean): Either[JsonObject, Vector[ResultEntry[JsonObject]]] = {

    def inner(result: JsonObject): Option[ResultEntry[JsonObject]] =
      result("_source").flatMap(_.asObject).map {
        case source if scored => ScoredResultEntry(result("_score").flatMap(_.as[Float].toOption).getOrElse(0), source)
        case source           => UnscoredResultEntry(source)
      }

    val hitsList = json.asJson.hcursor.downField("hits").getOrElse("hits")(Vector.empty[JsonObject]).leftMap(_ => json)
    hitsList.flatMap { vector =>
      vector.foldM(Vector.empty[ResultEntry[JsonObject]])((acc, json) => inner(json).map(acc :+ _).toRight(json))
    }
  }

  private def token(json: JsonObject): Option[String] = {
    val hits   = json.asJson.hcursor.downField("hits").downField("hits")
    val length = hits.values.fold(1)(_.size)
    hits.downN(length - 1).downField("sort").focus.map(_.noSpaces)
  }

  private def decodeScoredResults(maxScore: Float): Decoder[SearchResults[JsonObject]] =
    Decoder.decodeJsonObject.emap { json =>
      queryResults(json, scored = true) match {
        case Right(list)   => Right(ScoredSearchResults(Hits.fetchTotal(json), maxScore, list, token(json)))
        case Left(errJson) => Left(s"Could not decode source from value '$errJson'")
      }
    }

  private val decodeUnscoredResults: Decoder[SearchResults[JsonObject]] =
    Decoder.decodeJsonObject.emap { json =>
      queryResults(json, scored = false) match {
        case Right(list)   => Right(UnscoredSearchResults(Hits.fetchTotal(json), list, token(json)))
        case Left(errJson) => Left(s"Could not decode source from value '$errJson'")
      }
    }

  implicit val decodeQueryResults: Decoder[SearchResults[JsonObject]] =
    Decoder.decodeJsonObject.flatMap(
      _.asJson.hcursor
        .downField("hits")
        .get[Float]("max_score")
        .toOption
        .filterNot(f => f.isInfinite || f.isNaN) match {
        case Some(maxScore) => decodeScoredResults(maxScore)
        case None           => decodeUnscoredResults
      }
    )

  final private[client] case class Count(value: Long)
  private[client] object Count {
    given Decoder[Count] = Decoder.instance(_.get[Long]("count").map(Count(_)))
  }
}
