package ai.senscience.nexus.delta.sdk.otel

import ai.senscience.nexus.delta.sdk.otel.OtelHttp4sAttributes.*
import cats.effect.kernel.Outcome
import cats.effect.{IO, MonadCancelThrow, Resource}
import fs2.Stream
import org.http4s.{Request, Response, Status}
import org.http4s.client.Client
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.semconv.attributes.HttpAttributes
import org.typelevel.otel4s.trace.{SpanKind, StatusCode, Tracer}

object OtelTracingClient {

  /**
    * Wraps `client` so each request is traced under `spanDef`. Responses whose status is in `allowedStatuses` (e.g. a
    * 404 the caller interprets itself, or a 409 on a create) are treated as normal outcomes and do not error the span.
    * Prefer the `client.traced(...)` / `client.tracedRecover(...)` syntax over calling this directly.
    */
  def apply(client: Client[IO], spanDef: SpanDef, allowedStatuses: Status*)(using Tracer[IO]): Client[IO] =
    make(client, spanDef, allowedStatuses.toSet)

  private def make(client: Client[IO], spanDef: SpanDef, allowedStatuses: Set[Status])(using Tracer[IO]): Client[IO] =
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
                    span.addAttributes(responseAttributes(response)) >>
                      IO.whenA(isError(response.status, allowedStatuses))(span.setStatus(StatusCode.Error))
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

  // A client span is errored on 4xx/5xx (per OTel semconv); 2xx/3xx never are. Statuses the caller declared as
  // allowed (e.g. 404 on an existence check, 409 on a create) are treated as normal and do not error the span.
  private def isError(status: Status, allowedStatuses: Set[Status]): Boolean =
    (status.responseClass == Status.ClientError || status.responseClass == Status.ServerError) &&
      !allowedStatuses.contains(status)

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
