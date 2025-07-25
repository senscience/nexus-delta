package ai.senscience.nexus.delta.elasticsearch.query

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.http.ResponseUtils.decodeBodyAsJson
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import akka.http.scaladsl.model.{StatusCode as AkkaStatusCode, StatusCodes}
import cats.effect.IO
import io.circe.syntax.KeyOps
import io.circe.{Encoder, Json, JsonObject}
import org.http4s.{Response, Status}

/**
  * Enumeration of errors raised while querying the Elasticsearch indices
  */
sealed abstract class ElasticSearchClientError(val reason: String, val body: Option[Json]) extends Rejection

object ElasticSearchClientError {

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
      decodeBodyAsJson(response).map { body =>
        ElasticsearchCreateIndexError(response.status, Some(body))
      }
  }

  final case class ElasticsearchQueryError(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The elasticsearch endpoint responded with a status: $status",
        body
      )

  object ElasticsearchQueryError {
    def apply(response: Response[IO]): IO[ElasticsearchQueryError] =
      decodeBodyAsJson(response).map { body =>
        ElasticsearchQueryError(response.status, Some(body))
      }
  }

  final case class ElasticsearchWriteError(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The elasticsearch endpoint responded with a status: $status",
        body
      )

  object ElasticsearchWriteError {
    def apply(response: Response[IO]): IO[ElasticsearchWriteError] =
      decodeBodyAsJson(response).map { body =>
        ElasticsearchWriteError(response.status, Some(body))
      }
  }

  final case class ScriptCreationDismissed(status: Status, override val body: Option[Json])
      extends ElasticSearchClientError(
        s"The script creation failed with a status: $status",
        body
      )

  object ScriptCreationDismissed {
    def apply(response: Response[IO]): IO[ScriptCreationDismissed] =
      decodeBodyAsJson(response).map { body =>
        ScriptCreationDismissed(response.status, Some(body))
      }
  }

  /**
    * Rejection returned when attempting to interact with a resource providing an id that cannot be resolved to an Iri.
    *
    * @param id
    *   the resource identifier
    */
  final case class InvalidResourceId(id: String)
      extends ElasticSearchClientError(s"Resource identifier '$id' cannot be expanded to an Iri.", None)

  implicit val elasticSearchClientErrorEncoder: Encoder.AsObject[ElasticSearchClientError] =
    Encoder.AsObject.instance { r =>
      val obj = JsonObject(keywords.tpe := ClassUtils.simpleName(r), "reason" := r.reason)
      r.body.flatMap(_.asObject).getOrElse(obj)
    }

  implicit final val elasticSearchClientErrorJsonLdEncoder: JsonLdEncoder[ElasticSearchClientError] =
    JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.error))

  implicit val elasticSearchClientErrorHttpResponseFields: HttpResponseFields[ElasticSearchClientError] =
    HttpResponseFields {
      case ElasticsearchActionError(status, _)      => AkkaStatusCode.int2StatusCode(status.code)
      case ElasticsearchCreateIndexError(status, _) => AkkaStatusCode.int2StatusCode(status.code)
      case ElasticsearchQueryError(status, _)       => AkkaStatusCode.int2StatusCode(status.code)
      case ElasticsearchWriteError(status, _)       => AkkaStatusCode.int2StatusCode(status.code)
      case InvalidResourceId(_)                     => StatusCodes.BadRequest
      case ScriptCreationDismissed(_, _)            => StatusCodes.InternalServerError
    }

}
