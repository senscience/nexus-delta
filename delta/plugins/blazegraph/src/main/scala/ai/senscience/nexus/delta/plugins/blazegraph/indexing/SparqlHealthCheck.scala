package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, ExecutionStrategy, ProjectionBackpressure, ProjectionMetadata, Supervisor}
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.{Signal, SignallingRef}

import java.time.Instant
import scala.concurrent.duration.DurationInt

trait SparqlHealthCheck {

  def healthy: Signal[IO, Boolean] = failing.map(!_)

  def failing: Signal[IO, Boolean]

}

object SparqlHealthCheck {

  private val logger = Logger[SparqlHealthCheck]

  sealed private trait SparqlHealth {
    def isFailing: Boolean
  }

  private object SparqlHealth {

    case object Healthy extends SparqlHealth {
      override def isFailing: Boolean = false
    }

    final case class Failing(since: Instant, lastLog: Instant) extends SparqlHealth {
      override def isFailing: Boolean = true
    }
  }

  private val metadata: ProjectionMetadata = ProjectionMetadata("system", "sparql-healthcheck", None, None)

  private[indexing] def stream(healthStream: Stream[IO, Boolean], failingSignal: SignallingRef[IO, Boolean]) =
    healthStream
      .evalScan(SparqlHealth.Healthy: SparqlHealth) {
        case (SparqlHealth.Healthy, true)           =>
          logger.debug("The sparql database is responding as expected").as(SparqlHealth.Healthy)
        case (SparqlHealth.Healthy, false)          =>
          IO.realTimeInstant.flatMap { now =>
            logger.error("The sparql database is not responding").as(SparqlHealth.Failing(now, now))
          }
        case (SparqlHealth.Failing(since, _), true) =>
          logger
            .info(s"The sparql database is responding again after failing since '$since'")
            .as(SparqlHealth.Healthy)
        case (f: SparqlHealth.Failing, false)       =>
          IO.realTimeInstant.flatMap { now =>
            if now.getEpochSecond - f.lastLog.getEpochSecond > 60L then {
              logger
                .error(s"The sparql database has not been responding since '${f.since}'")
                .as(SparqlHealth.Failing(f.since, now))
            } else {
              IO.pure(f)
            }
          }
      }
      .evalMap { health =>
        failingSignal.set(health.isFailing)
      }
      .drain

  def apply(client: SparqlClient, supervisor: Supervisor): IO[SparqlHealthCheck] =
    SignallingRef.of[IO, Boolean](true).flatMap { failingSignal =>
      given ProjectionBackpressure       = ProjectionBackpressure.Noop
      val healthCheck: SparqlHealthCheck = new SparqlHealthCheck {
        override def failing: SignallingRef[IO, Boolean] = failingSignal
      }
      supervisor
        .run(
          CompiledProjection.fromStream(
            metadata,
            ExecutionStrategy.EveryNode,
            _ => stream(client.healthCheck(1.second), failingSignal)
          )
        )
        .as(healthCheck)
    }
}
