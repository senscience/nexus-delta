package ai.senscience.nexus.delta

import ai.senscience.nexus.delta.config.{HttpConfig, StrictEntity}
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.utils.IOFuture
import ai.senscience.nexus.delta.otel.OtelPekkoMetrics
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.plugin.Plugin
import ai.senscience.nexus.delta.sourcing.stream.config.ProjectionConfig
import ai.senscience.nexus.delta.sourcing.stream.config.ProjectionConfig.ClusterConfig
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import izumi.distage.model.Locator
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.cors.scaladsl.CorsDirectives.cors
import org.apache.pekko.http.cors.scaladsl.settings.CorsSettings
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route, RouteResult}
import org.typelevel.otel4s.metrics.MeterProvider
import org.typelevel.otel4s.oteljava.OtelJava

import scala.concurrent.duration.DurationInt

object BootstrapPekko {

  private val logger = Logger[BootstrapPekko.type]

  private def routes(locator: Locator, otelMetrics: OtelPekkoMetrics, clusterConfig: ClusterConfig): Route = {
    import org.apache.pekko.http.scaladsl.server.Directives.*
    import sdk.directives.UriDirectives.*
    val nodeHeader = RawHeader("X-Delta-Node", clusterConfig.nodeIndex.toString)
    otelMetrics.serverMetrics {
      respondWithHeader(nodeHeader) {
        cors(locator.get[CorsSettings]) {
          handleExceptions(locator.get[ExceptionHandler]) {
            handleRejections(locator.get[RejectionHandler]) {
              uriPrefix(locator.get[BaseUri].base) {
                decodeRequest {
                  encodeResponse {
                    val (strict, rest) = locator.get[Set[PriorityRoute]].partition(_.requiresStrictEntity)
                    concat(
                      concat(rest.toVector.sortBy(_.priority).map(_.route)*),
                      locator.get[StrictEntity].apply() {
                        concat(strict.toVector.sortBy(_.priority).map(_.route)*)
                      }
                    )
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def apply(locator: Locator, plugins: List[Plugin]): Resource[IO, Unit] = {
    given as: ActorSystem             = locator.get[ActorSystem]
    val http: HttpConfig              = locator.get[HttpConfig]
    val projections: ProjectionConfig = locator.get[ProjectionConfig]
    val otel: OtelJava[IO]            = locator.get[OtelJava[IO]]
    given MeterProvider[IO]           = otel.meterProvider

    def startHttpServer(otelMetrics: OtelPekkoMetrics) = IOFuture.defaultCancelable(
      IO(
        Http()
          .newServerAt(
            http.interface,
            http.port
          )
          .bindFlow(RouteResult.routeToFlow(routes(locator, otelMetrics, projections.cluster)))
      )
    )

    val acquire = {
      for {
        _       <- logger.info("Booting up service....")
        metrics <- OtelPekkoMetrics()
        binding <- startHttpServer(metrics)
        _       <- logger.info(s"Bound to ${binding.localAddress.getHostString}:${binding.localAddress.getPort}")
      } yield ()
    }.recoverWith { th =>
      logger.error(th)(
        s"Failed to perform an http binding on ${http.interface}:${http.port}"
      ) >> plugins
        .traverse(_.stop())
        .timeout(30.seconds)
    }.void

    val release = IO.fromFuture(IO(as.terminate())).void

    Resource.make(acquire)(_ => release)
  }

}
