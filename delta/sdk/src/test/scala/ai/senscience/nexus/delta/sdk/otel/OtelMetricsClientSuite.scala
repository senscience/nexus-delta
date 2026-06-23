package ai.senscience.nexus.delta.sdk.otel

import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Resource}
import io.opentelemetry.sdk.metrics.data.MetricData
import org.http4s.client.Client
import org.http4s.{Request, Response, Status, Uri}
import org.typelevel.otel4s.metrics.MeterProvider
import org.typelevel.otel4s.oteljava.context.Context
import org.typelevel.otel4s.oteljava.testkit.metrics.MetricsTestkit

import scala.jdk.CollectionConverters.*

class OtelMetricsClientSuite extends NexusSuite {

  private val requestDuration      = "http.client.request.duration"
  private val activeRequests       = "http.client.active_requests"
  private val abnormalTerminations = "http.client.abnormal_terminations"

  private val request = Request[IO](uri = Uri.unsafeFromString("http://localhost:9200/index"))

  private def respondingWith(status: Status): Client[IO] = Client[IO](_ => Resource.pure(Response[IO](status)))

  /**
    * Runs `test` with an [[OtelMetricsClient]] backed by an in-memory SDK, handing it a way to collect the metrics that
    * were emitted.
    */
  private def withMetricsClient(test: (OtelMetricsClient, IO[Seq[MetricData]]) => IO[Unit]): IO[Unit] =
    MetricsTestkit.inMemory[IO]().use { testkit =>
      given MeterProvider[IO] = testkit.meterProvider
      OtelMetricsClient("test").flatMap(client => test(client, testkit.collectMetrics))
    }

  extension (metrics: Seq[MetricData]) {
    private def get(name: String): Option[MetricData]   = metrics.find(_.getName == name)
    private def histogramCount(name: String): Long      =
      get(name).fold(0L)(_.getHistogramData.getPoints.asScala.map(_.getCount).sum)
    private def longSum(name: String): Long             =
      get(name).fold(0L)(_.getLongSumData.getPoints.asScala.map(_.getValue).sum)
    private def errorType(name: String): Option[AnyRef] =
      get(name)
        .flatMap(_.getHistogramData.getPoints.asScala.headOption)
        .flatMap(_.getAttributes.asMap.asScala.collectFirst { case (k, v) if k.getKey == "error.type" => v })
  }

  test("the noop metrics client returns the underlying client unchanged") {
    val client = respondingWith(Status.Ok)
    assert(OtelMetricsClient.noop.wrap(client, "test") eq client)
  }

  test("a successful response records the request duration and no abnormal termination") {
    withMetricsClient { (metricsClient, collect) =>
      val client = metricsClient.wrap(respondingWith(Status.Ok), "test")
      client.run(request).use_ >> collect.map { metrics =>
        assertEquals(metrics.histogramCount(requestDuration), 1L)
        assertEquals(metrics.histogramCount(abnormalTerminations), 0L)
      }
    }
  }

  test("a 4xx response is recorded as a completed request, not an abnormal termination") {
    withMetricsClient { (metricsClient, collect) =>
      val client = metricsClient.wrap(respondingWith(Status.NotFound), "test")
      client.run(request).use_ >> collect.map { metrics =>
        assertEquals(metrics.histogramCount(requestDuration), 1L)
        assertEquals(metrics.histogramCount(abnormalTerminations), 0L)
        assertEquals(metrics.errorType(requestDuration), Some("404"))
      }
    }
  }

  test("a failed request records an abnormal termination tagged with the error type and no request duration") {
    val failingClient: Client[IO] = Client[IO](_ => Resource.eval(IO.raiseError(new RuntimeException("boom"))))
    withMetricsClient { (metricsClient, collect) =>
      val client = metricsClient.wrap(failingClient, "test")
      client.run(request).use_.attempt >> collect.map { metrics =>
        assertEquals(metrics.histogramCount(abnormalTerminations), 1L)
        assertEquals(metrics.histogramCount(requestDuration), 0L)
        assertEquals(metrics.errorType(abnormalTerminations), Some("java.lang.RuntimeException"))
      }
    }
  }

  test("a cancelled request records an abnormal termination tagged as a cancellation") {
    withMetricsClient { (metricsClient, collect) =>
      IO.deferred[Unit].flatMap { acquired =>
        val never  = Client[IO](_ => Resource.eval(acquired.complete(()) >> IO.never[Response[IO]]))
        val client = metricsClient.wrap(never, "test")
        for {
          fiber   <- client.run(request).use_.start
          _       <- acquired.get
          _       <- fiber.cancel
          metrics <- collect
        } yield {
          assertEquals(metrics.histogramCount(abnormalTerminations), 1L)
          assertEquals(metrics.histogramCount(requestDuration), 0L)
          assertEquals(metrics.errorType(abnormalTerminations), Some("cancel"))
        }
      }
    }
  }

  test("the active requests counter returns to zero once a request completes") {
    withMetricsClient { (metricsClient, collect) =>
      val client = metricsClient.wrap(respondingWith(Status.Ok), "test")
      client.run(request).use_ >> collect.map(metrics => assertEquals(metrics.longSum(activeRequests), 0L))
    }
  }

}
