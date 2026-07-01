package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.{extractParentSpanContext, serverSpan}
import ai.senscience.nexus.delta.sdk.directives.RouteClassifier.*
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.effect.IO
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.api.trace.{SpanId, SpanKind, StatusCode}
import io.opentelemetry.sdk.trace.data.SpanData
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, Route}
import org.typelevel.otel4s.oteljava.testkit.trace.TracesTestkit
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.Future

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

  // Names paths under `myresource`; the base-uri prefix `v1` is stripped before classifying.
  private val classifier = RouteClassifier { route("myresource" / str("id")) }
  private val prefix     = Some(Label.unsafe("v1"))

  extension (span: SpanData) {
    private def statusCode: StatusCode                       = span.getStatus.getStatusCode
    private def stringAttribute(key: String): Option[String] =
      Option(span.getAttributes.get(AttributeKey.stringKey(key)))
    private def longAttribute(key: String): Option[Long]     =
      Option(span.getAttributes.get(AttributeKey.longKey(key))).map(_.longValue)
  }

  "The serverSpan directive" should {

    "trace a completed request as a non-errored server span named by the classifier (prefix stripped)" in {
      val spans = tracedSpans {
        val route = serverSpan(classifier, prefix) { complete(StatusCodes.OK) }
        Get("/v1/myresource/abc") ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      }
      val span  = singleSpan(spans)
      span.getName shouldEqual "GET myresource/<str:id>"
      span.getKind shouldEqual SpanKind.SERVER
      span.statusCode shouldEqual StatusCode.UNSET
      span.stringAttribute("http.route") shouldEqual Some("myresource/<str:id>")
      span.longAttribute("http.response.status_code") shouldEqual Some(200L)
    }

    "leave the span unset for a 4xx-mapped domain exception (mapped below, observed as the final response)" in {
      val spans = tracedSpans {
        val handler        = ExceptionHandler { case _: RuntimeException => complete(StatusCodes.NotFound) }
        val raising: Route = _ => Future.failed(new RuntimeException("not found"))
        val route          = serverSpan(classifier, prefix) { handleExceptions(handler)(raising) }
        Get("/v1/myresource/abc") ~> route ~> check {
          status shouldEqual StatusCodes.NotFound
        }
      }
      val span  = singleSpan(spans)
      span.statusCode shouldEqual StatusCode.UNSET
      span.stringAttribute("error.type") shouldEqual None
      span.longAttribute("http.response.status_code") shouldEqual Some(404L)
    }

    "mark the span as errored on a 5xx response" in {
      val spans = tracedSpans {
        val route = serverSpan(classifier, prefix) { complete(StatusCodes.InternalServerError) }
        Get("/v1/myresource/abc") ~> route ~> check {
          status shouldEqual StatusCodes.InternalServerError
        }
      }
      val span  = singleSpan(spans)
      span.statusCode shouldEqual StatusCode.ERROR
      span.stringAttribute("error.type") shouldEqual Some("500 Internal Server Error")
    }

    "mark the span as errored on an unhandled exception" in {
      val spans = tracedSpans {
        val failing: Route = _ => Future.failed(new RuntimeException("boom"))
        Get("/v1/myresource/abc") ~> Route.seal(serverSpan(classifier, prefix)(failing)) ~> check {
          status shouldEqual StatusCodes.InternalServerError
        }
      }
      val span  = singleSpan(spans)
      span.statusCode shouldEqual StatusCode.ERROR
      span.stringAttribute("error.type") shouldEqual Some("java.lang.RuntimeException")
    }

    "join the trace propagated through the request headers" in {
      val traceId      = "0af7651916cd43dd8448eb211c80319c"
      val parentSpanId = "b7ad6b7169203331"
      val traceparent  = RawHeader("traceparent", s"00-$traceId-$parentSpanId-01")
      val spans        = tracedSpans {
        val route = serverSpan(classifier, prefix) { complete(StatusCodes.OK) }
        Get("/v1/myresource/abc").withHeaders(traceparent) ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      }
      val span         = singleSpan(spans)
      // the server span continues the upstream trace and is a child of the incoming span
      span.getTraceId shouldEqual traceId
      span.getParentSpanId shouldEqual parentSpanId
    }

    "expose the created span as the parent span context to the inner route" in {
      val spans = tracedSpans {
        val route = serverSpan(classifier, prefix) {
          extractParentSpanContext {
            case Some(_) => complete("has-parent")
            case None    => complete(StatusCodes.InternalServerError, "no-parent")
          }
        }
        Get("/v1/myresource/abc") ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[String] shouldEqual "has-parent"
        }
      }
      val span  = singleSpan(spans)
      // a child span created downstream (via the request-attribute bridge) is parented to this server span
      span.getParentSpanId shouldEqual SpanId.getInvalid
    }

    "not create a span for a path the classifier does not match" in {
      val spans = tracedSpans {
        val route = serverSpan(classifier, prefix) { complete(StatusCodes.OK) }
        Get("/v1/unmatched") ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      }
      spans shouldBe empty
    }
  }

}
