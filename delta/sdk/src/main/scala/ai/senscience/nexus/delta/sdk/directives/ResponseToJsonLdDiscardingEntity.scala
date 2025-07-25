package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{conditionalCache, requestEncoding}
import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling.*
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.{MediaTypes, StatusCode}
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.Encoder
import io.circe.syntax.*

sealed trait ResponseToJsonLdDiscardingEntity {
  def apply(statusOverride: Option[StatusCode]): Route
}

object ResponseToJsonLdDiscardingEntity extends DiscardValueInstances {

  private[directives] def apply[A: JsonLdEncoder: Encoder](
      io: IO[Complete[A]]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLdDiscardingEntity =
    new ResponseToJsonLdDiscardingEntity {

      private def fallbackAsPlainJson =
        onSuccess(io.unsafeToFuture()) { case Complete(status, headers, entityTag, value) =>
          requestEncoding { encoding =>
            conditionalCache(entityTag, MediaTypes.`application/json`, encoding) {
              complete(status, headers, value.asJson)
            }
          }
        }

      override def apply(statusOverride: Option[StatusCode]): Route =
        extractRequest { request =>
          extractMaterializer { implicit mat =>
            request.discardEntityBytes()
            ResponseToJsonLd.fromComplete(io).apply(statusOverride) ~ fallbackAsPlainJson
          }
        }
    }
}

sealed trait DiscardValueInstances extends DiscardLowPriorityValueInstances {

  implicit def ioValue[A: JsonLdEncoder: Encoder](
      io: IO[A]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLdDiscardingEntity =
    ResponseToJsonLdDiscardingEntity(io.map(Complete(OK, Seq.empty, None, _)))

  implicit def valueWithHttpResponseFields[A: JsonLdEncoder: HttpResponseFields: Encoder](
      value: A
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLdDiscardingEntity =
    ResponseToJsonLdDiscardingEntity(IO.pure(Complete(value)))

}

sealed trait DiscardLowPriorityValueInstances {
  implicit def valueWithoutHttpResponseFields[A: JsonLdEncoder: Encoder](
      value: A
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLdDiscardingEntity =
    ResponseToJsonLdDiscardingEntity(IO.pure(Complete(OK, Seq.empty, None, value)))

}
