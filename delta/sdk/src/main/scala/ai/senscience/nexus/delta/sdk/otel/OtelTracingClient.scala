package ai.senscience.nexus.delta.sdk.otel

import ai.senscience.nexus.delta.sdk.otel.OtelHttp4sAttributes.*
import cats.effect.kernel.Outcome
import cats.effect.{IO, MonadCancelThrow, Resource}
import fs2.Stream
import org.http4s.{Request, Response}
import org.http4s.client.Client
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.semconv.attributes.HttpAttributes
import org.typelevel.otel4s.trace.{SpanKind, StatusCode, Tracer}

object OtelTracingClient {

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

  private def requestAttributes(request: Request[IO]): Attributes = {
    val uri        = request.uri
    val attributes = Attributes.newBuilder
    attributes += requestMethod(request)
    attributes ++= serverAddress(uri.host)
    attributes ++= serverPort(request.remotePort, uri)
    attributes += uriFull(uri)
    attributes ++= uriScheme(uri.scheme)
    request.remote.foreach { socketAddress =>
      attributes += networkPeerAddress(socketAddress.host)
      attributes += networkPeerPort(socketAddress.port)
    }
    attributes ++= headers(request.headers, HttpAttributes.HttpRequestHeader)
    attributes.result()
  }

  private def responseAttributes(response: Response[IO]): Attributes = {
    val attributes = Attributes.newBuilder
    attributes += statusCode(response.status)
    attributes ++= errorType(response.status)
    attributes ++= headers(response.headers, HttpAttributes.HttpResponseHeader)
    attributes.result()
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
