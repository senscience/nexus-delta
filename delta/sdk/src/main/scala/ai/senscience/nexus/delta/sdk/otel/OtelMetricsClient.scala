package ai.senscience.nexus.delta.sdk.otel

import ai.senscience.nexus.delta.sdk.otel.OtelHttp4sAttributes.*
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.http4s.client.Client
import org.http4s.{Request, Response}
import org.typelevel.otel4s.{Attribute, AttributeKey, Attributes}
import org.typelevel.otel4s.metrics.MeterProvider

import scala.concurrent.duration.FiniteDuration

trait OtelMetricsClient {

  def wrap(client: Client[IO], traffic: String): Client[IO]

}

object OtelMetricsClient {

  private val trafficKey = AttributeKey[String]("nexus.http.client.traffic")

  private case object Disabled extends OtelMetricsClient {
    def wrap(client: Client[IO], category: String): Client[IO] = client
  }

  final private class Enabled(metrics: HttpMetricsCollection) extends OtelMetricsClient {
    import metrics.*

    def wrap(client: Client[IO], traffic: String): Client[IO] = Client[IO] { (request: Request[IO]) =>
      val trafficAttribute = Attribute(trafficKey, traffic)
      val reqAttributes    = requestAttributes(request) + trafficAttribute

      for {
        start    <- Resource.monotonic[IO]
        _        <- recordActiveRequests(reqAttributes)
        response <- client
                      .run(request)
                      .onCancel(registerCancel(start))
                      .onError(registerError(start, _))
        end      <- Resource.monotonic[IO]
        _        <- onSuccess(response, end - start, reqAttributes)
      } yield response
    }

    private def requestAttributes(request: Request[IO]): Attributes = {
      val uri        = request.uri
      val attributes = Attributes.newBuilder
      attributes += requestMethod(request)
      attributes ++= serverAddress(uri.host)
      attributes ++= serverPort(request.remotePort, uri)
      attributes ++= uriScheme(uri.scheme)
      attributes.result()
    }

    private def onSuccess(response: Response[IO], duration: FiniteDuration, reqAttributes: Attributes) = {
      val status        = response.status
      val allAttributes = reqAttributes + statusCode(status) ++ errorType(status)
      Resource.eval {
        requestDuration.record(
          secondsFromDuration(duration),
          allAttributes
        ) >> IO.unlessA(status.isSuccess) {
          abnormalTerminations.record(
            secondsFromDuration(duration),
            allAttributes
          )
        }
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

    private def recordActiveRequests(attributes: Attributes) =
      Resource.make(activeRequests.inc(attributes))(_ => activeRequests.dec(attributes))
  }

  def apply(version: String)(using MeterProvider[IO]): IO[OtelMetricsClient] =
    MeterProvider[IO]
      .meter("ai.senscience.nexus.delta.otel.client.http")
      .withVersion(version)
      .get
      .flatMap { meter =>
        meter.meta.isEnabled.flatMap {
          case false => IO.pure(Disabled)
          case true  => HttpMetricsCollection("client", meter).map(Enabled(_))
        }
      }

  def noop: OtelMetricsClient = Disabled

}
