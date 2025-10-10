package ai.senscience.nexus.delta.otel

import ai.senscience.nexus.delta.config.BuildInfo
import ai.senscience.nexus.delta.sdk.otel.OtelAttributes.*
import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.*
import cats.syntax.all.*
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.server.{Directive0, Directives, RouteResult}
import org.apache.pekko.http.scaladsl.server.Directives.{extractRequest, mapRouteResultFuture, pass}
import org.apache.pekko.http.scaladsl.server.RouteResult.{Complete, Rejected}
import org.typelevel.otel4s.{Attribute, Attributes}
import org.typelevel.otel4s.metrics.{BucketBoundaries, Histogram, MeterProvider, UpDownCounter}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait OtelMetrics {

  def serverMetrics: Directive0

}

object OtelMetrics {

  // Defined in https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#metric-httpserverrequestduration
  private val bucketBoundaries = BucketBoundaries(
    Vector(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10)
  )

  private case object Disabled extends OtelMetrics {
    override def serverMetrics: Directive0 = pass
  }

  final class Enabled(
      requestDuration: Histogram[IO, Double],
      activeRequests: UpDownCounter[IO, Long],
      abnormalTerminations: Histogram[IO, Double]
  ) extends OtelMetrics {
    override def serverMetrics: Directive0 =
      extractRequest.flatMap { request =>
        mapRouteResultFuture { result =>
          recordMetrics(request, result).use(IO.pure).unsafeToFuture()
        }
      }

    private def recordMetrics(request: HttpRequest, result: Future[RouteResult]): Resource[IO, RouteResult] = {
      val attributes = Attributes(requestMethod(request), requestMethod(request))
      for {
        start  <- Resource.monotonic[IO]
        _      <- recordActiveRequests(attributes)
        result <- Resource
                    .eval(IO.fromFuture(IO.delay(result)))
                    .onCancel(registerCancel(start))
                    .onError(registerError(start, _))
        end    <- Resource.monotonic[IO]
        _      <- onSuccess(result, end - start, attributes)
      } yield result
    }

    private def recordActiveRequests(attributes: Attributes) =
      Resource.make(activeRequests.inc(attributes))(_ => activeRequests.dec(attributes))

    private def onSuccess(routeResult: RouteResult, duration: FiniteDuration, attributes: Attributes) =
      Resource.eval {
        routeResult match {
          case Complete(response)   =>
            requestDuration.record(
              secondsFromDuration(duration),
              attributes + responseStatus(response) ++ errorTypeFromStatus(response)
            ) >> IO.whenA(serverError(response)) {
              abnormalTerminations.record(
                secondsFromDuration(duration),
                attributes ++ errorTypeFromStatus(response)
              )
            }
          case Rejected(rejections) =>
            abnormalTerminations.record(
              secondsFromDuration(duration),
              attributes + errorType("reject")
            )
        }
      }

    private def registerCancel(start: FiniteDuration) =
      Resource.monotonic[IO].evalMap { now =>
        val duration = now - start
        abnormalTerminations.record(
          secondsFromDuration(duration),
          errorType("cancel")
        )
      }

    private def registerError(start: FiniteDuration, error: Throwable) =
      Resource.monotonic[IO].evalMap { now =>
        val duration = now - start
        abnormalTerminations.record(
          secondsFromDuration(duration),
          errorType(error)
        )
      }
  }

  def apply()(using MeterProvider[IO]): IO[OtelMetrics] =
    MeterProvider[IO]
      .meter("ai.senscience.nexus.delta.otel.server.http")
      .withVersion(BuildInfo.version)
      .get
      .flatMap { meter =>
        meter.meta.isEnabled.flatMap {
          case false => IO.pure(Disabled)
          case true  =>
            val requestDuration: IO[Histogram[IO, Double]] =
              meter
                .histogram[Double](s"http.server.request.duration")
                .withUnit("s")
                .withDescription(s"Duration of HTTP server requests.")
                .withExplicitBucketBoundaries(bucketBoundaries)
                .create

            val activeRequests: IO[UpDownCounter[IO, Long]] =
              meter
                .upDownCounter[Long](s"http.server.active_requests")
                .withUnit("{request}")
                .withDescription(s"Number of active HTTP server requests.")
                .create

            val abnormalTerminations: IO[Histogram[IO, Double]] =
              meter
                .histogram[Double](s"http.server.abnormal_terminations")
                .withUnit("s")
                .withDescription(s"Duration of HTTP server abnormal terminations.")
                .withExplicitBucketBoundaries(bucketBoundaries)
                .create

            (requestDuration, activeRequests, abnormalTerminations).mapN(Enabled.apply)
        }
      }

  private def secondsFromDuration(duration: FiniteDuration): Double =
    duration.toMillis / 1000.0
}
