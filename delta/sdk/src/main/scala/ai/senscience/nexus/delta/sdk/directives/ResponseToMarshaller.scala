package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.{childSpan, extractParentSpanContext}
import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import org.apache.pekko.http.scaladsl.marshalling.ToEntityMarshaller
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes.OK
import org.apache.pekko.http.scaladsl.server.Directives.{complete, onSuccess}
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.{SpanContext, Tracer}

trait ResponseToMarshaller {
  def apply(statusOverride: Option[StatusCode]): Route
}

object ResponseToMarshaller extends RdfMarshalling {

  private[directives] def apply[A: ToEntityMarshaller](io: IO[Complete[A]])(using Tracer[IO]): ResponseToMarshaller =
    (statusOverride: Option[StatusCode]) => {

      val ioFinal = io.map(value => value.copy(status = statusOverride.getOrElse(value.status)))

      def ioRoute(spanContext: Option[SpanContext]) = childSpan(spanContext, "emit") {
        ioFinal.map { v =>
          complete(v.status, v.headers, v.value)
        }
      }
      extractParentSpanContext { spanContext =>
        onSuccess(ioRoute(spanContext).unsafeToFuture())(identity)
      }
    }

  implicit def ioEntityMarshaller[A: ToEntityMarshaller](io: IO[A])(using Tracer[IO]): ResponseToMarshaller =
    ResponseToMarshaller(io.map(v => Complete(OK, Seq.empty, None, v)))
}
