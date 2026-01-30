package ai.senscience.nexus.delta.plugins.blazegraph.client

import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphErrorParser
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import cats.effect.IO
import io.circe.syntax.KeyOps
import io.circe.{Encoder, JsonObject}
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.http4s.Status.ClientError
import org.http4s.{EntityDecoder, Response, Status}

import java.net.ConnectException
import scala.concurrent.TimeoutException

/**
  * Error that can occur when using an [[SparqlClient]]
  */
sealed abstract class SparqlClientError(val reason: String, val details: Option[String])
    extends Exception
    with Product
    with Serializable {
  override def fillInStackTrace(): SparqlClientError = this

  override def getMessage: String = toString

  override def toString: String =
    reason ++ details.map(d => s"\ndetails '$d'").getOrElse("")

}

object SparqlClientError {

  final case class SparqlConnectError(cause: ConnectException)
      extends SparqlClientError(s"The sparql engine can't be reached: '${cause.getMessage}'", None)

  final case class SparqlTimeoutError(cause: TimeoutException)
      extends SparqlClientError(s"The request to sparql resulted in a timeout: '${cause.getMessage}'", None)

  case object SparqlUnknownHost extends SparqlClientError("The hostname for the sparql engine can't be resolved", None)

  final case class SparqlActionError(status: Status, action: String)
      extends SparqlClientError(
        s"The sparql $action failed with status $status",
        None
      )

  final case class SparqlQueryError(status: Status, body: String)
      extends SparqlClientError(
        s"The sparql endpoint responded with a status: $status",
        Some(body)
      )

  object SparqlQueryError {
    def apply(response: Response[IO]): IO[SparqlQueryError] =
      EntityDecoder.decodeText(response).map { body =>
        SparqlQueryError(response.status, body)
      }

    def blazegraph(response: Response[IO]): IO[SparqlQueryError] =
      EntityDecoder.decodeText(response).map { body =>
        val parsedBody = BlazegraphErrorParser.parse(body)
        SparqlQueryError(response.status, parsedBody)
      }
  }

  final case class SparqlWriteError(status: Status, body: String)
      extends SparqlClientError(
        s"The sparql endpoint responded with a status: $status",
        Some(body)
      ) {

    def isClientError: Boolean = status.responseClass == ClientError
  }

  object SparqlWriteError {
    def apply(response: Response[IO]): IO[SparqlWriteError] =
      EntityDecoder.decodeText(response).map { body =>
        SparqlWriteError(response.status, body)
      }
  }

  /**
    * Error when trying to perform a count on an index
    */
  final case class InvalidCountRequest(index: String, queryString: String)
      extends SparqlClientError(
        s"Attempting to count the triples the index '$index' with a wrong query '$queryString'",
        None
      )

  /**
    * Error when trying to perform an update and the query passed is wrong.
    */
  final case class InvalidUpdateRequest(index: String, queryString: String, override val details: Option[String])
      extends SparqlClientError(
        s"Attempting to update the index '$index' with a wrong query '$queryString'",
        details
      )

  given Encoder[SparqlClientError] = Encoder.AsObject.instance { e =>
    JsonObject(
      keywords.tpe := "SparqlClientError",
      "reason"     := e.reason,
      "details"    := e.details
    )
  }

  given JsonLdEncoder[SparqlClientError] =
    JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.error))

  given HttpResponseFields[SparqlClientError] =
    HttpResponseFields {
      case SparqlQueryError(status, _) => StatusCode.int2StatusCode(status.code)
      case _                           => StatusCodes.InternalServerError
    }
}
