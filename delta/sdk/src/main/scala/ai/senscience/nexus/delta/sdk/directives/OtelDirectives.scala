package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.sdk.otel.OtelHeaders
import ai.senscience.nexus.delta.sdk.otel.OtelPekkoAttributes.*
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.unsafe.implicits.*
import org.apache.pekko.http.scaladsl.model.{HttpHeader, HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.server
import org.apache.pekko.http.scaladsl.server.Directives.{extractRequest, mapRouteResultFuture}
import org.apache.pekko.http.scaladsl.server.RouteResult.{Complete, Rejected}
import org.apache.pekko.http.scaladsl.server.{Directive0, RouteResult}
import org.typelevel.otel4s.context.propagation.TextMapGetter
import org.typelevel.otel4s.semconv.attributes.*
import org.typelevel.otel4s.trace.{SpanKind, StatusCode, Tracer}
import org.typelevel.otel4s.{Attribute, Attributes}

import java.util.Locale

object OtelDirectives {

  given TextMapGetter[HttpRequest] =
    new TextMapGetter[HttpRequest] {
      def get(carrier: HttpRequest, key: String): Option[String] =
        carrier.headers.find(_.name == key).map(_.value)

      def keys(carrier: HttpRequest): Iterable[String] = carrier.headers.map(_.name)
    }

  def routeSpan(route: String)(using Tracer[IO]): Directive0 = {
    extractRequest.flatMap { request =>
      mapRouteResultFuture { result =>
        Tracer[IO].meta.isEnabled
          .flatMap {
            case true  =>
              IO.uncancelable { poll =>
                Tracer[IO].joinOrRoot(request) {
                  val spanName = s"${request.method.name()} $route"
                  Tracer[IO]
                    .spanBuilder(spanName)
                    .withSpanKind(SpanKind.Server)
                    .addAttributes(requestAttributes(request, route))
                    .build
                    .use { span =>
                      poll { IO.fromFuture(IO.delay(result)) }
                        .guaranteeCase {
                          case Outcome.Succeeded(fa) =>
                            fa.flatMap {
                              case Complete(response)   =>
                                span.addAttributes(responseAttributes(response)) >>
                                  IO.whenA(serverError(response))(span.setStatus(StatusCode.Error))
                              case Rejected(rejections) =>
                                val description = rejections.map(_.getClass.getSimpleName).mkString(",")
                                span.setStatus(StatusCode.Error, description)
                            }
                          case Outcome.Errored(e)    =>
                            span.addAttributes(
                              ErrorAttributes.ErrorType(e.getClass.getName)
                            ) >> span.setStatus(StatusCode.Error)
                          case Outcome.Canceled()    =>
                            IO.unit
                        }
                    }
                }
              }
            case false => IO.fromFuture(IO.delay(result))
          }
          .unsafeToFuture()
      }
    }
  }

  private def requestAttributes(request: HttpRequest, route: String) = {
    val attributes = Attributes.newBuilder
    attributes += originalScheme(request)
    attributes += requestMethod(request)
    attributes ++= urlPath(request.uri.path)
    attributes += HttpAttributes.HttpRoute(route)
    attributes ++= urlQuery(request.uri.query())
    attributes += ClientAttributes.ClientPort(request.uri.effectivePort)
    attributes += ClientAttributes.ClientAddress(request.uri.authority.host.address)
    attributes ++= networkProtocolName(request)
    attributes ++= networkProtocolVersion(request)
    attributes ++= headers(request.headers)
    attributes.result()
  }

  private def headers(headers: Seq[HttpHeader]) =
    headers
      .groupMap(_.name)(_.value)
      .view
      .flatMap { case (name, values) =>
        Option.when(OtelHeaders.default.contains(name)) {
          val key = HttpAttributes.HttpRequestHeader.transformName(_ + "." + name.toLowerCase(Locale.ROOT))
          Attribute(key, values)
        }
      }

  private def responseAttributes(response: HttpResponse) = {
    val attributes = Attributes.newBuilder
    attributes ++= errorTypeFromStatus(response)
    attributes += responseStatus(response)
    attributes ++= headers(response.headers)
    attributes.result()
  }
}
