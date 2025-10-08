package ai.senscience.nexus.delta.sdk.directives

import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.unsafe.implicits.*
import org.apache.pekko.http.scaladsl.model.headers.`X-Forwarded-Proto`
import org.apache.pekko.http.scaladsl.model.{HttpHeader, HttpRequest, HttpResponse, Uri}
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

  private val defaultHeaders = Set(
    "Accept",
    "Accept-CH",
    "Accept-Charset",
    "Accept-CH-Lifetime",
    "Accept-Encoding",
    "Accept-Language",
    "Accept-Ranges",
    "Access-Control-Allow-Credentials",
    "Access-Control-Allow-Headers",
    "Access-Control-Allow-Origin",
    "Access-Control-Expose-Methods",
    "Access-Control-Max-Age",
    "Access-Control-Request-Headers",
    "Access-Control-Request-Method",
    "Age",
    "Allow",
    "Alt-Svc",
    "B3",
    "Cache-Control",
    "Clear-Site-Data",
    "Connection",
    "Content-Disposition",
    "Content-Encoding",
    "Content-Language",
    "Content-Length",
    "Content-Location",
    "Content-Range",
    "Content-Security-Policy",
    "Content-Security-Policy-Report-Only",
    "Content-Type",
    "Correlation-ID",
    "Cross-Origin-Embedder-Policy",
    "Cross-Origin-Opener-Policy",
    "Cross-Origin-Resource-Policy",
    "Date",
    "Deprecation",
    "Device-Memory",
    "DNT",
    "Early-Data",
    "ETag",
    "Expect",
    "Expect-CT",
    "Expires",
    "Feature-Policy",
    "Forwarded",
    "From",
    "Host",
    "If-Match",
    "If-Modified-Since",
    "If-None-Match",
    "If-Range",
    "If-Unmodified-Since",
    "Keep-Alive",
    "Large-Allocation",
    "Last-Modified",
    "Link",
    "Location",
    "Max-Forwards",
    "Origin",
    "Pragma",
    "Proxy-Authenticate",
    "Public-Key-Pins",
    "Public-Key-Pins-Report-Only",
    "Range",
    "Referer",
    "Referer-Policy",
    "Retry-After",
    "Save-Data",
    "Sec-CH-UA",
    "Sec-CH-UA-Arch",
    "Sec-CH-UA-Bitness",
    "Sec-CH-UA-Full-Version",
    "Sec-CH-UA-Full-Version-List",
    "Sec-CH-UA-Mobile",
    "Sec-CH-UA-Model",
    "Sec-CH-UA-Platform",
    "Sec-CH-UA-Platform-Version",
    "Sec-Fetch-Dest",
    "Sec-Fetch-Mode",
    "Sec-Fetch-Site",
    "Sec-Fetch-User",
    "Server",
    "Server-Timing",
    "SourceMap",
    "Strict-Transport-Security",
    "TE",
    "Timing-Allow-Origin",
    "Tk",
    "Trailer",
    "Transfer-Encoding",
    "Upgrade",
    "Vary",
    "Via",
    "Viewport-Width",
    "Warning",
    "Width",
    "X-B3-Sampled",
    "X-B3-SpanId",
    "X-B3-TraceId",
    "X-Content-Type-Options",
    "X-Correlation-ID",
    "X-DNS-Prefetch-Control",
    "X-Download-Options",
    "X-Forwarded-For",
    "X-Forwarded-Host",
    "X-Forwarded-Port",
    "X-Forwarded-Proto",
    "X-Forwarded-Scheme",
    "X-Frame-Options",
    "X-Permitted-Cross-Domain-Policies",
    "X-Powered-By",
    "X-Real-IP",
    "X-Request-ID",
    "X-Request-Start",
    "X-Runtime",
    "X-Scheme",
    "X-SourceMap",
    "X-XSS-Protection"
  )

  given TextMapGetter[HttpRequest] =
    new TextMapGetter[HttpRequest] {
      def get(carrier: HttpRequest, key: String): Option[String] =
        carrier.headers.find(_.name == key).map(_.value)

      def keys(carrier: HttpRequest): Iterable[String] = carrier.headers.map(_.name)
    }

  def routeSpan(route: String)(using Tracer[IO]): Directive0 = {
    extractRequest.flatMap { request =>
      mapRouteResultFuture { result =>
        val pouet = Tracer[IO].meta.isEnabled
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
        pouet
      }
    }
  }

  private def requestAttributes(request: HttpRequest, route: String) = {
    val attributes = Attributes.newBuilder
    attributes += originalScheme(request)
    attributes += method(request)
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

  private def originalScheme(request: HttpRequest) = {
    val originalScheme = request.header[`X-Forwarded-Proto`].map(_.protocol).getOrElse(request.uri.scheme)
    UrlAttributes.UrlScheme(originalScheme)
  }

  private def method(request: HttpRequest) =
    HttpAttributes.HttpRequestMethod(request.method.name())

  private def urlPath(path: Uri.Path) =
    Option.unless(path.isEmpty)(UrlAttributes.UrlPath(path.toString))

  private def urlQuery(query: Uri.Query) =
    Option.unless(query.isEmpty)(UrlAttributes.UrlQuery(query.toString))

  private def networkProtocolName(request: HttpRequest) =
    Option.when(request.protocol.value.startsWith("HTTP/"))(NetworkAttributes.NetworkProtocolName("http"))

  private def networkProtocolVersion(request: HttpRequest) = {
    val protocol = request.protocol.value
    Option.when(protocol.startsWith("HTTP/"))(
      NetworkAttributes.NetworkProtocolVersion(protocol.substring("HTTP/".length))
    )
  }

  private def headers(headers: Seq[HttpHeader]) =
    headers
      .groupMap(_.name)(_.value)
      .view
      .flatMap { case (name, values) =>
        Option.when(defaultHeaders.contains(name)) {
          val key = HttpAttributes.HttpRequestHeader.transformName(_ + "." + name.toLowerCase(Locale.ROOT))
          Attribute(key, values)
        }
      }

  private def responseAttributes(response: HttpResponse) = {
    val attributes = Attributes.newBuilder
    if serverError(response) then {
      attributes += ErrorAttributes.ErrorType(response.status.value)
    }
    attributes += HttpAttributes.HttpResponseStatusCode(response.status.intValue)
    attributes ++= headers(response.headers)
    attributes.result()
  }

  private def serverError(response: HttpResponse) = response.status.intValue > 500
}
