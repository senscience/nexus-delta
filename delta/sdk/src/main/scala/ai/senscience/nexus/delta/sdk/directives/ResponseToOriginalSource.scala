package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{conditionalCache, requestEncoding}
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.{childSpan, extractParentSpanContext}
import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling.jsonSourceCodec
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.pekko.http.scaladsl.marshalling.ToEntityMarshaller
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.server.Directives.{complete, onSuccess}
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.{SpanContext, Tracer}

/**
  * Handles serialization of [[OriginalSource]] and generates the appropriate response headers
  */
trait ResponseToOriginalSource {
  def apply(): Route
}

object ResponseToOriginalSource extends RdfMarshalling {

  private given JsonValueCodec[Json] = jsonSourceCodec

  implicit private def originalSourceMarshaller(using BaseUri, JsonKeyOrdering): ToEntityMarshaller[OriginalSource] = {
    jsonMarshaller.compose(_.asJson)
  }

  private[directives] def apply(
      io: IO[Complete[OriginalSource]]
  )(using BaseUri, JsonKeyOrdering, Tracer[IO]): ResponseToOriginalSource =
    () => {
      def ioRoute(spanContext: Option[SpanContext]) =
        childSpan(spanContext, "emitOriginalSource") {
          io.map { v =>
            requestEncoding { encoding =>
              conditionalCache(v.entityTag, MediaTypes.`application/json`, encoding) {
                complete(v.status, v.headers, v.value)
              }
            }
          }
        }

      extractParentSpanContext { spanContext =>
        onSuccess(ioRoute(spanContext).unsafeToFuture())(identity)
      }
    }

  implicit def ioResponseOriginalPayloadValue(
      io: IO[OriginalSource]
  )(using BaseUri, JsonKeyOrdering, Tracer[IO]): ResponseToOriginalSource =
    ResponseToOriginalSource(io.map(Complete(_)))
}
