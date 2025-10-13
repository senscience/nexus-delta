package ai.senscience.nexus.delta.sdk.otel

import ai.senscience.nexus.delta.sdk.otel.OtelHttp4sAttributes.{errorType, requestAttributes, responseAttributes}
import cats.effect.kernel.Outcome
import cats.effect.{IO, MonadCancelThrow, Resource}
import fs2.Stream
import org.http4s.Request
import org.http4s.client.Client
import org.typelevel.otel4s.trace.{SpanKind, StatusCode, Tracer}

object OtelClient {

  def apply(client: Client[IO], spanDef: SpanDef)(using Tracer[IO]) =
    Client[IO] { (request: Request[IO]) =>
      Resource.eval(Tracer[IO].meta.isEnabled).flatMap { enabled =>
        if !enabled then client.run(request)
        else {
          MonadCancelThrow[Resource[IO, *]].uncancelable { poll =>
            buildSpan(spanDef, request).flatMap { spanRes =>
              val span = spanRes.span
              poll(client.run(request)).guaranteeCase {
                case Outcome.Succeeded(fa) =>
                  fa.evalMap { response =>
                    val respAttributes = responseAttributes(response)
                    span.addAttributes(respAttributes) >>
                      IO.unlessA(response.status.isSuccess) {
                        span.setStatus(StatusCode.Error)
                      }
                  }
                case Outcome.Errored(e)    =>
                  Resource.eval(
                    span.addAttributes(errorType(e)) >>
                      span.setStatus(StatusCode.Error)
                  )
                case Outcome.Canceled()    =>
                  Resource.unit
              }
            }
          }
        }
      }
    }

  private def buildSpan(span: SpanDef, request: Request[IO])(using Tracer[IO]) = {
    val reqNoBody     = request.withBodyStream(Stream.empty)
    val spanName      = s"${reqNoBody.method.name} ${span.name}"
    val reqAttributes = requestAttributes(reqNoBody)
    Tracer[IO]
      .spanBuilder(spanName)
      .withSpanKind(SpanKind.Client)
      .addAttributes(span.attributes ++ reqAttributes)
      .build
      .resource
  }

}
