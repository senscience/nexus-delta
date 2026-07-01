package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.directives.SpanName.value
import ai.senscience.nexus.delta.sdk.otel.OtelHeaders
import ai.senscience.nexus.delta.sdk.otel.OtelPekkoAttributes.*
import ai.senscience.nexus.delta.sourcing.model.Label
import cats.effect.IO
import cats.syntax.all.*
import cats.effect.kernel.Outcome
import cats.effect.unsafe.implicits.*
import org.apache.pekko.http.scaladsl.model.{AttributeKey, HttpHeader, HttpRequest, HttpResponse, Uri}
import org.apache.pekko.http.scaladsl.server
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.RouteResult.{Complete, Rejected}
import org.apache.pekko.http.scaladsl.server.{Directive, Directive0, Directive1, RouteResult}
import org.typelevel.otel4s.context.propagation.TextMapGetter
import org.typelevel.otel4s.semconv.attributes.*
import org.typelevel.otel4s.trace.*
import org.typelevel.otel4s.{Attribute, Attributes}

import java.util.Locale
import scala.annotation.nowarn
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
    // `attribute` relies on a JavaMapping implicit that Scala 3.10 will no longer resolve; revisit at that bump.
    extractRequest.map(_.attribute(parentSpanContextKey): @nowarn("cat=deprecation"))

  /**
    * A single server span at the root of the route tree, named by the `classifier` (from the request path, with the
    * base-uri `prefix` stripped first). Sitting above the exception/rejection handlers, it observes the final response,
    * so a 4xx-mapped domain exception is classified as a client error (span left unset) while genuine 5xx failures are
    * flagged — matching the OTel semconv for server spans. Paths the classifier does not name are not traced.
    */
  def serverSpan(classifier: RouteClassifier, prefix: Option[Label])(using Tracer[IO]): Directive0 =
    extractRequest.flatMap { request =>
      classifier(stripPrefix(request.uri.path, prefix)) match {
        case None           => pass
        case Some(spanName) =>
          onSuccess(buildSpan(request, spanName.value).unsafeToFuture()).flatMap {
            case Some(span) =>
              mapRequest(_.addAttribute(parentSpanContextKey, span.context)) &
                mapRouteResultFuture(result => updateSpanOnResult(result, span).unsafeToFuture())
            case None       => pass
          }
      }
    }

  /** Drops the leading base-uri prefix segment (if present) so the classifier sees the same path the routes do. */
  private def stripPrefix(path: Uri.Path, prefix: Option[Label]): Uri.Path =
    prefix match {
      case Some(label) =>
        val p = label.value
        path match {
          case Uri.Path.Slash(Uri.Path.Segment(`p`, tail)) => tail
          case Uri.Path.Segment(`p`, tail)                 => tail
          case other                                       => other
        }
      case None        => path
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
                // A rejection at this route is pekko backtracking: a sibling/parent route may still handle the request,
                // and an unrecovered rejection is turned into a 4xx by the top-level RejectionHandler — a client error,
                // not a server-span failure. Log it for debugging, but don't mark the span as errored. Genuine failures
                // remain flagged above (5xx responses) and below (unhandled exceptions).
                logger.debug(s"Route was rejected with: ${rejections.map(_.getClass.getSimpleName).mkString(", ")}")
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
