package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{conditionalCache, requestEncoding}
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.{childSpan, extractParentSpanContext}
import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.Encoder
import io.circe.syntax.EncoderOps
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes.OK
import org.apache.pekko.http.scaladsl.server.Directives.{complete, onSuccess}
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.{SpanContext, Tracer}

trait ResponseToJson {
  def apply(): Route
}

object ResponseToJson extends RdfMarshalling {

  private[directives] def apply[A: Encoder](
      io: IO[Complete[A]]
  )(using JsonKeyOrdering, Tracer[IO]): ResponseToJson =
    () => {
      def ioRoute(spanContext: Option[SpanContext]) =
        childSpan(spanContext, "emitJson") {
          io.map { v =>
            requestEncoding { encoding =>
              conditionalCache(v.entityTag, MediaTypes.`application/json`, encoding) {
                complete(v.status, v.headers, v.value.asJson)
              }
            }
          }
        }
      extractParentSpanContext { spanContext =>
        onSuccess(ioRoute(spanContext).unsafeToFuture())(identity)
      }
    }

  implicit def ioResponseJson[A: Encoder](
      io: IO[A]
  )(using JsonKeyOrdering, Tracer[IO]): ResponseToJson =
    ResponseToJson(io.map(v => Complete(OK, Seq.empty, None, v)))

}
