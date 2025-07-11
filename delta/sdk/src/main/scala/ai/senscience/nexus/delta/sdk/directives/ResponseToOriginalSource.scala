package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{conditionalCache, requestEncoding}
import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling.jsonSourceCodec
import ai.senscience.nexus.delta.sdk.marshalling.{OriginalSource, RdfMarshalling}
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.server.Directives.{complete, onSuccess}
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.syntax.EncoderOps

/**
  * Handles serialization of [[OriginalSource]] and generates the appropriate response headers
  */
trait ResponseToOriginalSource {
  def apply(): Route
}

object ResponseToOriginalSource extends RdfMarshalling {

  implicit private def originalSourceMarshaller(implicit
      ordering: JsonKeyOrdering
  ): ToEntityMarshaller[OriginalSource] =
    jsonMarshaller(ordering, jsonSourceCodec).compose(_.asJson)

  private[directives] def apply(
      io: IO[Complete[OriginalSource]]
  )(implicit jo: JsonKeyOrdering): ResponseToOriginalSource =
    () => {
      val ioRoute = io.map { v =>
        requestEncoding { encoding =>
          conditionalCache(v.entityTag, MediaTypes.`application/json`, encoding) {
            complete(v.status, v.headers, v.value)
          }
        }
      }
      onSuccess(ioRoute.unsafeToFuture())(identity)
    }

  implicit def ioResponseOriginalPayloadValue(
      io: IO[OriginalSource]
  )(implicit jo: JsonKeyOrdering): ResponseToOriginalSource =
    ResponseToOriginalSource(io.map(Complete(_)))
}
