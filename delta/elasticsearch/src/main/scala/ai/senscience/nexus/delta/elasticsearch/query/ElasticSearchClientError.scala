package ai.senscience.nexus.delta.elasticsearch.query

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.http.ResponseUtils.decodeBodyAsJson
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import cats.effect.IO
import io.circe.syntax.KeyOps
import io.circe.{Encoder, Json, JsonObject}
import org.apache.pekko.http.scaladsl.model.{StatusCode as PekkoStatusCode, StatusCodes}
import org.http4s.{Response, Status}

import java.net.ConnectException
import scala.concurrent.TimeoutException

/**
  * Enumeration of errors raised while querying the Elasticsearch indices
  */
sealed abstract class ElasticSearchClientError(val reason: String, val body: Option[Json]) extends Rejection

object ElasticSearchClientError {

  final case class ElasticSearchConnectError(cause: ConnectException)
      extends ElasticSearchClientError(s"elasticsearch can't be reached: ${cause.getMessage}", None)

  final case class ElasticSearchTimeoutError(cause: TimeoutException)
      extends ElasticSearchClientError(s"The request to elasticsearch resulted in a timeout: ${cause.getMessage}", None)

  case object ElasticSearchUnknownHost
      extends ElasticSearchClientError("The hostname for elasticsearch can't be resolved", None)

  final case class ElasticsearchActionError(status: Status, action: String)
      extends ElasticSearchClientError(
        s"The elasticsearch $action failed with status $status",
        None
      )

  final case class ElasticsearchCreateIndexError(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The elasticsearch endpoint responded with a status: $status",
        body
      )

  object ElasticsearchCreateIndexError {
    def apply(response: Response[IO]): IO[ElasticsearchCreateIndexError] =
      fromResponse(response)(ElasticsearchCreateIndexError(_, _))
  }

  final case class ElasticsearchQueryError(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The elasticsearch endpoint responded with a status: $status",
        body
      )

  object ElasticsearchQueryError {
    def apply(response: Response[IO]): IO[ElasticsearchQueryError] =
      fromResponse(response)(ElasticsearchQueryError(_, _))
  }

  final case class ElasticsearchWriteError(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The elasticsearch endpoint responded with a status: $status",
        body
      )

  object ElasticsearchWriteError {
    def apply(response: Response[IO]): IO[ElasticsearchWriteError] =
      fromResponse(response)(ElasticsearchWriteError(_, _))
  }

  final case class ElasticsearchUpdateMappingError(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The elasticsearch endpoint responded with a status: $status",
        body
      )

  object ElasticsearchUpdateMappingError {
    def apply(response: Response[IO]): IO[ElasticsearchUpdateMappingError] =
      fromResponse(response)(ElasticsearchUpdateMappingError(_, _))
  }

  final case class ElasticsearchUpdateSettingsError(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The elasticsearch endpoint responded with a status: $status",
        body
      )

  object ElasticsearchUpdateSettingsError {
    def apply(response: Response[IO]): IO[ElasticsearchUpdateSettingsError] =
      fromResponse(response)(ElasticsearchUpdateSettingsError(_, _))
  }

  final case class ScriptCreationDismissed(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The script creation failed with a status: $status",
        body
      )

  object ScriptCreationDismissed {
    def apply(response: Response[IO]): IO[ScriptCreationDismissed] =
      fromResponse(response)(ScriptCreationDismissed(_, _))
  }

  private def fromResponse[E](response: Response[IO])(f: (s: Status, json: Option[Json]) => E) =
    decodeBodyAsJson(response).map { body => f(response.status, Some(body)) }

  /**
    * Rejection returned when attempting to interact with a resource providing an id that cannot be resolved to an Iri.
    *
    * @param id
    *   the resource identifier
    */
  final case class InvalidResourceId(id: String)
      extends ElasticSearchClientError(s"Resource identifier '$id' cannot be expanded to an Iri.", None)

  given Encoder.AsObject[ElasticSearchClientError] =
    Encoder.AsObject.instance { r =>
      val obj = JsonObject(keywords.tpe := ClassUtils.simpleName(r), "reason" := r.reason)
      r.body.flatMap(_.asObject).getOrElse(obj)
    }

  given JsonLdEncoder[ElasticSearchClientError] =
    JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.error))

  given HttpResponseFields[ElasticSearchClientError] =
    HttpResponseFields {
      case ElasticsearchActionError(status, _)      => PekkoStatusCode.int2StatusCode(status.code)
      case ElasticsearchCreateIndexError(status, _) => PekkoStatusCode.int2StatusCode(status.code)
      case ElasticsearchQueryError(status, _)       => PekkoStatusCode.int2StatusCode(status.code)
      case ElasticsearchWriteError(status, _)       => PekkoStatusCode.int2StatusCode(status.code)
      case InvalidResourceId(_)                     => StatusCodes.BadRequest
      case _                                        => StatusCodes.InternalServerError
    }

}
