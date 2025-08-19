package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{conditionalCache, requestEncoding}
import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives.{complete, onSuccess}
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.Encoder
import io.circe.syntax.EncoderOps

trait ResponseToJson {
  def apply(): Route
}

object ResponseToJson extends RdfMarshalling {

  private[directives] def apply[A: Encoder](
      io: IO[Complete[A]]
  )(implicit jo: JsonKeyOrdering): ResponseToJson =
    () => {
      val ioRoute = io.map { v =>
        requestEncoding { encoding =>
          conditionalCache(v.entityTag, MediaTypes.`application/json`, encoding) {
            complete(v.status, v.headers, v.value.asJson)
          }
        }
      }
      onSuccess(ioRoute.unsafeToFuture())(identity)
    }

  implicit def ioResponseJson[A: Encoder](
      io: IO[A]
  )(implicit jo: JsonKeyOrdering): ResponseToJson =
    ResponseToJson(io.map(v => Complete(OK, Seq.empty, None, v)))

}
