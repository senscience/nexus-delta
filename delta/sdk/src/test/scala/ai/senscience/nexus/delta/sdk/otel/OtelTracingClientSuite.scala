package ai.senscience.nexus.delta.sdk.otel

import ai.senscience.nexus.delta.sdk.otel.syntax.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Resource}
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import io.opentelemetry.sdk.trace.data.SpanData
import org.http4s.client.Client
import org.http4s.{Request, Response, Status, Uri}
import org.typelevel.otel4s.oteljava.testkit.trace.TracesTestkit
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.Tracer

class OtelTracingClientSuite extends NexusSuite {

  private val request = Request[IO](uri = Uri.unsafeFromString("http://localhost:9200/index"))
  private val spanDef = SpanDef("test", Attribute("nexus.test.attr", "value"))

  private def respondingWith(status: Status): Client[IO] =
    Client[IO](_ => Resource.pure(Response[IO](status)))

  private val failingClient: Client[IO] =
    Client[IO](_ => Resource.eval(IO.raiseError(new RuntimeException("boom"))))

  /**
    * Runs `test` with an otel4s [[Tracer]] backed by an in-memory SDK, handing it a way to collect the spans that were
    * finished.
    */
  private def withTracer(test: Tracer[IO] => IO[List[SpanData]] => IO[Unit]): IO[Unit] =
    TracesTestkit.inMemory[IO]().use { testkit =>
      testkit.tracerProvider.tracer("test").get.flatMap(tracer => test(tracer)(testkit.finishedSpans))
    }

  extension (span: SpanData) {
    private def statusCode: StatusCode                       = span.getStatus.getStatusCode
    private def stringAttribute(key: String): Option[String] =
      Option(span.getAttributes.get(AttributeKey.stringKey(key)))
    private def longAttribute(key: String): Option[Long]     =
      Option(span.getAttributes.get(AttributeKey.longKey(key))).map(_.longValue)
  }

  test("a successful request is traced as a non-errored client span carrying the request and response attributes") {
    withTracer { tracer => finishedSpans =>
      given Tracer[IO] = tracer
      val client       = respondingWith(Status.Ok).traced(spanDef)
      client.run(request).use_ >> finishedSpans.map {
        case span :: Nil =>
          assertEquals(span.getName, "GET test")
          assertEquals(span.getKind, SpanKind.CLIENT)
          // per OTel semconv a successful client span is left UNSET (OK is reserved for explicit app-level confirmation)
          assertEquals(span.statusCode, StatusCode.UNSET)
          assertEquals(span.stringAttribute("nexus.test.attr"), Some("value"))
          assertEquals(span.stringAttribute("http.request.method"), Some("GET"))
          assertEquals(span.longAttribute("http.response.status_code"), Some(200L))
          assertEquals(span.stringAttribute("url.full"), Some("http://localhost:9200/index"))
        case spans       => fail(s"expected a single span, got $spans")
      }
    }
  }

  test("a server error marks the span as errored and records the error type") {
    withTracer { tracer => finishedSpans =>
      given Tracer[IO] = tracer
      val client       = respondingWith(Status.InternalServerError).traced(spanDef)
      client.run(request).use_ >> finishedSpans.map { spans =>
        assert(
          spans.exists(span =>
            span.statusCode == StatusCode.ERROR && span.stringAttribute("error.type").contains("500")
          ),
          s"expected an errored span tagged 500, got $spans"
        )
      }
    }
  }

  test("tracedRecover does not error the span on a 404 or 409") {
    withTracer { tracer => finishedSpans =>
      given Tracer[IO]        = tracer
      def run(status: Status) = respondingWith(status).tracedRecover(spanDef).run(request).use_
      run(Status.NotFound) >> run(Status.Conflict) >> finishedSpans.map { spans =>
        assertEquals(spans.size, 2)
        assert(spans.forall(_.statusCode == StatusCode.UNSET), s"expected no errored span, got $spans")
      }
    }
  }

  test("tracedRecover still errors the span on a server error") {
    withTracer { tracer => finishedSpans =>
      given Tracer[IO] = tracer
      val client       = respondingWith(Status.InternalServerError).tracedRecover(spanDef)
      client.run(request).use_ >> finishedSpans.map { spans =>
        assert(spans.exists(_.statusCode == StatusCode.ERROR), s"expected an errored span, got $spans")
      }
    }
  }

  test("a failed request marks the span as errored and records the error type") {
    withTracer { tracer => finishedSpans =>
      given Tracer[IO] = tracer
      val client       = failingClient.traced(spanDef)
      client.run(request).use_.attempt >> finishedSpans.map { spans =>
        assert(
          spans.exists { span =>
            span.statusCode == StatusCode.ERROR && span
              .stringAttribute("error.type")
              .contains("java.lang.RuntimeException")
          },
          s"expected an errored span tagged with the exception type, got $spans"
        )
      }
    }
  }

  test("tracing is a no-op pass-through when the tracer is disabled") {
    given Tracer[IO] = Tracer.noop[IO]
    val client       = respondingWith(Status.Ok).traced(spanDef)
    client.run(request).use(response => IO(assertEquals(response.status, Status.Ok)))
  }

}
