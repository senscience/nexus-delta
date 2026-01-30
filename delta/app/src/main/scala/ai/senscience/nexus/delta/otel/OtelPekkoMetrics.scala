package ai.senscience.nexus.delta.otel

import ai.senscience.nexus.delta.config.BuildInfo
import ai.senscience.nexus.delta.sdk.otel.{secondsFromDuration, HttpMetricsCollection}
import ai.senscience.nexus.delta.sdk.otel.OtelPekkoAttributes.*
import cats.effect.unsafe.implicits.*
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.server.Directives.{extractRequest, mapRouteResultFuture, pass}
import org.apache.pekko.http.scaladsl.server.RouteResult.{Complete, Rejected}
import org.apache.pekko.http.scaladsl.server.{Directive0, RouteResult}
import org.typelevel.otel4s.metrics.MeterProvider
import org.typelevel.otel4s.Attributes

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait OtelPekkoMetrics {

  def serverMetrics: Directive0

}

object OtelPekkoMetrics {

  private case object Disabled extends OtelPekkoMetrics {
    override def serverMetrics: Directive0 = pass
  }

  final class Enabled(metrics: HttpMetricsCollection) extends OtelPekkoMetrics {
    import metrics.*

    override def serverMetrics: Directive0 =
      extractRequest.flatMap { request =>
        mapRouteResultFuture { result =>
          recordMetrics(request, result).use(IO.pure).unsafeToFuture()
        }
      }

    private def recordMetrics(request: HttpRequest, result: Future[RouteResult]): Resource[IO, RouteResult] = {
      val attributes = Attributes(requestMethod(request), originalScheme(request))
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

  def apply()(using MeterProvider[IO]): IO[OtelPekkoMetrics] =
    MeterProvider[IO]
      .meter("ai.senscience.nexus.delta.otel.server.http")
      .withVersion(BuildInfo.version)
      .get
      .flatMap { meter =>
        meter.meta.isEnabled.flatMap {
          case false => IO.pure(Disabled)
          case true  => HttpMetricsCollection("server", meter).map(Enabled(_))
        }
      }
}
