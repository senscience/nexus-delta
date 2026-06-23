package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.{extractParentSpanContext, routeSpan}
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.effect.IO
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import io.opentelemetry.sdk.trace.data.SpanData
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.{Route, ValidationRejection}
import org.typelevel.otel4s.oteljava.testkit.trace.TracesTestkit
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

class OtelDirectivesSpec extends CatsEffectSpec with RouteHelpers {

  /**
    * Builds and exercises a route under a [[Tracer]] backed by an in-memory SDK, then returns the spans that were
    * finished. The route is built and run with the testkit tracer in scope, shadowing the no-op tracer from
    * [[CatsEffectSpec]].
    */
  private def tracedSpans(checkRoute: Tracer[IO] ?=> Any): List[SpanData] =
    TracesTestkit
      .inMemory[IO](_.addTextMapPropagators(W3CTraceContextPropagator.getInstance()))
      .use { testkit =>
        testkit.tracerProvider.tracer("test").get.flatMap { tracer =>
          IO.blocking(checkRoute(using tracer)) >> testkit.finishedSpans
        }
      }
      .accepted

  private def singleSpan(spans: List[SpanData]): SpanData = spans match {
    case span :: Nil => span
    case other       => fail(s"expected a single span, got $other")
  }

  extension (span: SpanData) {
    private def statusCode: StatusCode                                = span.getStatus.getStatusCode
    private def stringAttribute(key: String): Option[String]          =
      Option(span.getAttributes.get(AttributeKey.stringKey(key)))
    private def longAttribute(key: String): Option[Long]              =
      Option(span.getAttributes.get(AttributeKey.longKey(key))).map(_.longValue)
    private def stringSeqAttribute(key: String): Option[List[String]] =
      Option(span.getAttributes.get(AttributeKey.stringArrayKey(key))).map(_.asScala.toList)
  }

  "The routeSpan directive" should {

    "trace a completed request as a non-errored server span carrying the route and response attributes" in {
      val spans = tracedSpans {
        val route = routeSpan("myroute") { complete(StatusCodes.OK) }
        Get("/resources?type=foo") ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      }
      val span  = singleSpan(spans)
      span.getName shouldEqual "GET myroute"
      span.getKind shouldEqual SpanKind.SERVER
      // per OTel semconv a successful server span is left UNSET (OK is reserved for explicit app-level confirmation)
      span.statusCode shouldEqual StatusCode.UNSET
      span.stringAttribute("http.route") shouldEqual Some("myroute")
      span.stringAttribute("http.request.method") shouldEqual Some("GET")
      span.stringAttribute("url.path") shouldEqual Some("/resources")
      span.stringAttribute("url.query") shouldEqual Some("type=foo")
      span.longAttribute("http.response.status_code") shouldEqual Some(200L)
    }

    "mark the span as errored and record the error type on a server error response" in {
      val spans = tracedSpans {
        val route = routeSpan("myroute") { complete(StatusCodes.BadGateway) }
        Get("/resources") ~> route ~> check {
          status shouldEqual StatusCodes.BadGateway
        }
      }
      val span  = singleSpan(spans)
      span.statusCode shouldEqual StatusCode.ERROR
      span.stringAttribute("error.type") shouldEqual Some("502 Bad Gateway")
    }

    "mark the span as errored and record the error type when the route result fails" in {
      val spans = tracedSpans {
        val failing: Route = _ => Future.failed(new RuntimeException("boom"))
        // sealing turns the failed route future into a 500; the span is errored beforehand, in the routeSpan wrapper
        Get("/resources") ~> Route.seal(routeSpan("myroute")(failing)) ~> check {
          status shouldEqual StatusCodes.InternalServerError
        }
      }
      val span  = singleSpan(spans)
      span.statusCode shouldEqual StatusCode.ERROR
      span.stringAttribute("error.type") shouldEqual Some("java.lang.RuntimeException")
    }

    "record allow-listed request headers as span attributes" in {
      val spans = tracedSpans {
        val route = routeSpan("myroute") { complete(StatusCodes.OK) }
        Get("/resources").withHeaders(RawHeader("Accept-Language", "fr")) ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      }
      singleSpan(spans).stringSeqAttribute("http.request.header.accept-language") shouldEqual Some(List("fr"))
    }

    "not mark the span as errored when the route is rejected" in {
      val spans = tracedSpans {
        val route = routeSpan("myroute") { reject(ValidationRejection("nope")) }
        Get("/resources") ~> route ~> check {
          handled shouldBe false
          rejection shouldEqual ValidationRejection("nope")
        }
      }
      singleSpan(spans).statusCode shouldEqual StatusCode.UNSET
    }

    "join the trace propagated through the request headers" in {
      val traceId      = "0af7651916cd43dd8448eb211c80319c"
      val parentSpanId = "b7ad6b7169203331"
      val traceparent  = RawHeader("traceparent", s"00-$traceId-$parentSpanId-01")
      val spans        = tracedSpans {
        val route = routeSpan("myroute") { complete(StatusCodes.OK) }
        Get("/resources").withHeaders(traceparent) ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      }
      val span         = singleSpan(spans)
      span.getTraceId shouldEqual traceId
      span.getParentSpanId shouldEqual parentSpanId
    }

    "expose the created span as the parent span context to the inner route" in {
      val spans = tracedSpans {
        val route = routeSpan("myroute") {
          extractParentSpanContext {
            case Some(_) => complete("has-parent")
            case None    => complete(StatusCodes.InternalServerError, "no-parent")
          }
        }
        Get("/resources") ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[String] shouldEqual "has-parent"
        }
      }
      spans.size shouldEqual 1
    }

    "pass through without creating a span when tracing is disabled" in {
      given Tracer[IO] = Tracer.noop[IO]
      val route        = routeSpan("myroute") { complete(StatusCodes.OK) }
      Get("/resources") ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }

}
