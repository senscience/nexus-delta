package ai.senscience.nexus.delta.sdk.otel

import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.{BucketBoundaries, Histogram, Meter, UpDownCounter}

final case class HttpMetricsCollection(
    requestDuration: Histogram[IO, Double],
    activeRequests: UpDownCounter[IO, Long],
    abnormalTerminations: Histogram[IO, Double]
)

object HttpMetricsCollection {

  // Defined in https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#metric-httpserverrequestduration
  private val bucketBoundaries = BucketBoundaries(
    Vector(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10)
  )

  def apply(kind: String, meter: Meter[IO]): IO[HttpMetricsCollection] = {
    val requestDuration: IO[Histogram[IO, Double]] =
      meter
        .histogram[Double](s"http.$kind.request.duration")
        .withUnit("s")
        .withDescription("Duration of HTTP server requests.")
        .withExplicitBucketBoundaries(bucketBoundaries)
        .create

    val activeRequests: IO[UpDownCounter[IO, Long]] =
      meter
        .upDownCounter[Long](s"http.$kind.active_requests")
        .withUnit("{request}")
        .withDescription("Number of active HTTP server requests.")
        .create

    val abnormalTerminations: IO[Histogram[IO, Double]] =
      meter
        .histogram[Double](s"http.$kind.abnormal_terminations")
        .withUnit("s")
        .withDescription("Duration of HTTP server abnormal terminations.")
        .withExplicitBucketBoundaries(bucketBoundaries)
        .create
    (requestDuration, activeRequests, abnormalTerminations).mapN(new HttpMetricsCollection(_, _, _))
  }

}
