package ai.senscience.nexus.delta.plugins.blazegraph.client

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import ai.senscience.nexus.delta.plugins.blazegraph.client.BlazegraphClient.timeoutHeader
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClientError.{InvalidCountRequest, SparqlActionError, SparqlQueryError, SparqlWriteError}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.{Aux, SparqlResultsJson}
import ai.senscience.nexus.delta.plugins.blazegraph.config.BlazegraphViewsConfig.OpentelemetryConfig
import ai.senscience.nexus.delta.plugins.blazegraph.model.NamespaceProperties
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sdk.otel.{OtelClient, SpanDef}
import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Stream
import org.http4s.Method.{DELETE, GET, POST}
import org.http4s.client.Client
import org.http4s.client.dsl.io.*
import org.http4s.{EntityDecoder, Header, MediaType, Status, Uri, UrlForm}
import org.typelevel.ci.CIString
import org.typelevel.otel4s.{Attribute, AttributeKey, Attributes}
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.duration.*
import scala.reflect.ClassTag

/**
  * A client that exposes additional functions on top of [[SparqlClient]] that are specific to Blazegraph.
  */
final class BlazegraphClient(client: Client[IO], endpoint: Uri, queryTimeout: Duration, otel: OpentelemetryConfig)(using
    Tracer[IO]
) extends SparqlClient {

  private val serviceVersion = """(buildVersion">)([^<]*)""".r
  private val serviceName    = "blazegraph"

  private val namespaceKey = AttributeKey[String]("nexus.sparql.namespace")

  private def withNamespace(namespace: String) =
    Attribute(namespaceKey, namespace)

  private val queryKey = AttributeKey[String]("nexus.sparql.query")

  private def withQuery(query: SparqlQuery) = Option.when(otel.captureQueries)(Attribute(queryKey, query.value))

  private val accessKey = AttributeKey[String]("nexus.sparql.access")
  private val read      = Attribute(accessKey, "read")
  private val write     = Attribute(accessKey, "read")

  private def queryEndpoint(namespace: String): Uri = endpoint / "namespace" / namespace / "sparql"

  private def updateEndpoint(namespace: String): Uri = queryEndpoint(namespace)

  override protected def queryRequest[A](
      namespace: String,
      q: SparqlQuery,
      mediaTypes: NonEmptyList[MediaType],
      additionalHeaders: Seq[Header.ToRaw]
  )(implicit entityDecoder: EntityDecoder[IO, A], classTag: ClassTag[A]): IO[A] = {
    val acceptHeader: Header.ToRaw = accept(mediaTypes)
    val attributes                 = Attributes.newBuilder
    attributes += withNamespace(namespace)
    attributes ++= withQuery(q)
    attributes += read
    val spanDef                    = SpanDef("namespace/<string:namespace>/sparql", attributes.result())
    val request                    =
      POST(queryEndpoint(namespace), (additionalHeaders.+:(acceptHeader))*).withEntity(UrlForm("query" -> q.value))
    OtelClient(client, spanDef).expectOr[A](request)(SparqlQueryError(_))
  }

  override def query[R <: SparqlQueryResponse](
      namespaces: Iterable[String],
      q: SparqlQuery,
      responseType: Aux[R],
      additionalHeaders: Seq[Header.ToRaw]
  ): IO[R] = {
    val timeout: Option[Header.ToRaw] =
      Option.when(queryTimeout.isFinite)(Header.Raw(timeoutHeader, queryTimeout.toMillis.toString))
    val headers                       = additionalHeaders ++ timeout
    super.query(namespaces, q, responseType, headers)
  }

  /**
    * Fetches the service description information (name and version)
    */
  override def serviceDescription: IO[ServiceDescription] =
    client
      .expect[ServiceDescription](endpoint / "status")
      .timeout(1.second)
      .recover(_ => ServiceDescription.unresolved(serviceName))

  override def healthCheck(period: FiniteDuration): Stream[IO, Boolean] =
    Stream.awakeEvery[IO](period) >>
      Stream.eval(
        client
          .successful(GET(endpoint / "status"))
          .timeout(1.second)
          .recover(_ => false)
      )

  override def existsNamespace(namespace: String): IO[Boolean] = {
    val spanDef = SpanDef("namespace/<string:namespace>", withNamespace(namespace), read)
    OtelClient(client, spanDef).statusFromUri(endpoint / "namespace" / namespace).flatMap {
      case Status.Ok       => IO.pure(true)
      case Status.NotFound => IO.pure(false)
      case status          => IO.raiseError(SparqlActionError(status, "exists"))
    }
  }

  /**
    * Attempts to create a namespace (if it doesn't exist) recovering gracefully when the namespace already exists.
    *
    * @param namespace
    *   the namespace
    * @param properties
    *   the properties to use for namespace creation
    * @return
    *   ''true'' wrapped on an IO when namespace has been created and ''false'' wrapped on an IO when it already existed
    */
  def createNamespace(namespace: String, properties: NamespaceProperties): IO[Boolean] = {
    val propWithNamespace = properties + ("com.bigdata.rdf.sail.namespace", namespace)
    val spanDef           = SpanDef("namespace/<string:namespace>", withNamespace(namespace), write)
    val request           = POST(endpoint / "namespace").withEntity(propWithNamespace.toString)
    OtelClient(client, spanDef).status(request).flatMap {
      case Status.Created  => IO.pure(true)
      case Status.Conflict => IO.pure(false)
      case Status.NotFound => IO.pure(false)
      case status          => IO.raiseError(SparqlActionError(status, "create"))
    }
  }

  override def createNamespace(namespace: String): IO[Boolean] =
    createNamespace(namespace, NamespaceProperties.defaultValue)

  override def deleteNamespace(namespace: String): IO[Boolean] = {
    val spanDef = SpanDef("namespace/<string:namespace>", withNamespace(namespace), write)
    val request = DELETE(endpoint / "namespace" / namespace)
    OtelClient(client, spanDef).status(request).flatMap {
      case Status.Ok       => IO.pure(true)
      case Status.NotFound => IO.pure(false)
      case status          => IO.raiseError(SparqlActionError(status, "delete"))
    }
  }

  def count(namespace: String): IO[Long] = {
    val sparqlQuery = SparqlConstructQuery.unsafe("SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }")
    query(Set(namespace), sparqlQuery, SparqlResultsJson)
      .flatMap { response =>
        val count = for {
          head          <- response.value.results.bindings.headOption
          countAsString <- head.get("count")
          count         <- countAsString.value.toLongOption
        } yield count
        IO.fromOption(count)(InvalidCountRequest(namespace, sparqlQuery.value))
      }
  }

  /**
    * List all namespaces in the blazegraph instance
    */
  def listNamespaces: IO[Vector[String]] = {
    val namespacePredicate = "http://www.bigdata.com/rdf#/features/KB/Namespace"
    val describeEndpoint   = (endpoint / "namespace").withQueryParam("describe-each-named-graph", "false")
    val spanDef            = SpanDef("namespace", read)
    val request            = GET(describeEndpoint, accept(SparqlResultsJson.mediaTypes))
    import ai.senscience.nexus.delta.kernel.http.circe.CirceEntityDecoder.*
    OtelClient(client, spanDef).expect[SparqlResults](request).map { response =>
      response.results.bindings.foldLeft(Vector.empty[String]) { case (acc, binding) =>
        val isNamespace   = binding.get("predicate").exists(_.value == namespacePredicate)
        val namespaceName = binding.get("object").map(_.value)
        if isNamespace then acc ++ namespaceName
        else acc
      }
    }
  }

  override def bulk(namespace: String, queries: Seq[SparqlWriteQuery]): IO[Unit] =
    IO.fromEither(SparqlBulkUpdate(namespace, queries)).flatMap { bulk =>
      val form     = UrlForm("update" -> bulk.queryString)
      val endpoint = updateEndpoint(namespace).copy(query = bulk.queryParams)

      val spanDef = SpanDef("namespace/<string:namespace>/sparql", withNamespace(namespace), write)
      val request = POST(endpoint, accept(SparqlResultsJson.mediaTypes)).withEntity(form)
      OtelClient(client, spanDef).expectOr[Unit](request)(SparqlWriteError(_)).void
    }

  private given EntityDecoder[IO, ServiceDescription] =
    EntityDecoder.text[IO].map {
      serviceVersion.findFirstMatchIn(_).map(_.group(2)) match {
        case None          => ServiceDescription.unresolved(serviceName)
        case Some(version) => ServiceDescription(serviceName, version)
      }
    }

}

object BlazegraphClient {

  /**
    * Blazegraph timeout header.
    */
  val timeoutHeader: CIString = CIString("X-BIGDATA-MAX-QUERY-MILLIS")
}
