package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.otel.OtelHeaders
import ai.senscience.nexus.delta.sdk.otel.OtelPekkoAttributes.*
import cats.effect.IO
import cats.syntax.all.*
import cats.effect.kernel.Outcome
import cats.effect.unsafe.implicits.*
import org.apache.pekko.http.scaladsl.model.{AttributeKey, HttpHeader, HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.server
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.RouteResult.{Complete, Rejected}
import org.apache.pekko.http.scaladsl.server.{Directive, Directive0, Directive1, RouteResult}
import org.typelevel.otel4s.context.propagation.TextMapGetter
import org.typelevel.otel4s.semconv.attributes.*
import org.typelevel.otel4s.trace.*
import org.typelevel.otel4s.{Attribute, Attributes}

import java.util.Locale
import scala.concurrent.Future

object OtelDirectives {

  private val logger = Logger[OtelDirectives.type]

  private val parentSpanContextKey: AttributeKey[SpanContext] = AttributeKey("parentSpanContext")

  given TextMapGetter[HttpRequest] =
    new TextMapGetter[HttpRequest] {
      def get(carrier: HttpRequest, key: String): Option[String] =
        carrier.headers.find(_.name == key).map(_.value)

      def keys(carrier: HttpRequest): Iterable[String] = carrier.headers.map(_.name)
    }

  def extractParentSpanContext: Directive1[Option[SpanContext]] =
    extractRequest.map(_.attribute(parentSpanContextKey))

  def routeSpan(route: String)(using Tracer[IO]): Directive0 = {
    extractRequest.flatMap { request =>
      onSuccess(
        buildSpan(request, route).unsafeToFuture()
      ).flatMap {
        case Some(span) =>
          // Setting this span as a parent for the downstream operations
          mapRequest {
            _.addAttribute(parentSpanContextKey, span.context)
          } &
            mapRouteResultFuture { result =>
              updateSpanOnResult(result, span).unsafeToFuture()
            }
        case None       => pass
      }
    }
  }

  private def buildSpan(request: HttpRequest, route: String)(using Tracer[IO]) =
    Tracer[IO].meta.isEnabled.flatMap { enabled =>
      Option.when(enabled)(s"${request.method.name()} $route").traverse { spanName =>
        Tracer[IO]
          .joinOrRoot(request) {
            Tracer[IO]
              .spanBuilder(spanName)
              .withSpanKind(SpanKind.Server)
              .addAttributes(requestAttributes(request, route))
              .build
              .startUnmanaged
          }
          .flatTap { span =>
            logger.debug(s"Created span span with context ${span.context.show} for uri ${request.uri} and route $route")
          }
      }
    }

  private def updateSpanOnResult(result: Future[RouteResult], span: Span[IO]) =
    logger.debug(s"Updating span with context ${span.context.show} with the response") >>
      IO.uncancelable { poll =>
        poll {
          IO.fromFuture(IO.delay(result))
        }.guaranteeCase {
          case Outcome.Succeeded(fa) =>
            fa.flatMap {
              case Complete(response)   =>
                span.addAttributes(responseAttributes(response)) >>
                  IO.whenA(serverError(response))(span.setStatus(StatusCode.Error))
              case Rejected(rejections) =>
                val description = rejections.map(_.getClass.getSimpleName).mkString(",")
                span.setStatus(StatusCode.Error, description)
            } >> span.end
          case Outcome.Errored(e)    =>
            span.addAttributes(
              ErrorAttributes.ErrorType(e.getClass.getName)
            ) >> span.setStatus(StatusCode.Error) >> span.end
          case Outcome.Canceled()    =>
            span.end
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

  def childSpan[A](parentContext: Option[SpanContext], spanName: String)(io: IO[A])(using Tracer[IO]): IO[A] =
    Tracer[IO].childOrContinue(parentContext) {
      Tracer[IO].span(spanName).surround(io)
    }
}
