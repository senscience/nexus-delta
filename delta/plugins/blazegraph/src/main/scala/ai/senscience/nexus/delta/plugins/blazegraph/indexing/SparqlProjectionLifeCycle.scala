package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategyConfig}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, PipeChain, ReferenceRegistry}
import cats.effect.IO
import fs2.Stream

import java.time.Instant
import scala.concurrent.duration.*

trait SparqlProjectionLifeCycle {

  def failing: Stream[IO, Boolean]

  def compile(view: ActiveViewDef): IO[CompiledProjection]

  def init(view: ActiveViewDef): IO[Unit]

  def destroy(view: ActiveViewDef): IO[Unit]

}

object SparqlProjectionLifeCycle {

  private val logger = Logger[SparqlProjectionLifeCycle]

  sealed private trait SparqlHealth {
    def isFailing: Boolean
  }

  private object SparqlHealth {

    object Healthy extends SparqlHealth {
      override def isFailing: Boolean = false
    }

    final case class Failing(since: Instant, lastLog: Instant) extends SparqlHealth {
      override def isFailing: Boolean = true
    }
  }

  def apply(
      graphStream: GraphResourceStream,
      registry: ReferenceRegistry,
      client: SparqlClient,
      retryStrategy: RetryStrategyConfig,
      batchConfig: BatchConfig
  )(implicit baseUri: BaseUri): SparqlProjectionLifeCycle = new SparqlProjectionLifeCycle {

    override def failing: Stream[IO, Boolean] = {
      client
        .healthCheck(1.second)
        .evalFold(SparqlHealth.Healthy: SparqlHealth) {
          case (SparqlHealth.Healthy, true)           => IO.pure(SparqlHealth.Healthy)
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
              if (now.getEpochSecond - f.lastLog.getEpochSecond > 60L) {
                logger
                  .error(s"The sparql database has not been responding since '${f.since}'")
                  .as(SparqlHealth.Failing(f.since, now))
              } else {
                IO.pure(f)
              }
            }
        }
        .map(_.isFailing)
    }

    override def compile(view: ActiveViewDef): IO[CompiledProjection] =
      IndexingViewDef.compile(view, compilePipeChain, graphStream, sink(view))

    override def init(view: ActiveViewDef): IO[Unit] =
      client.createNamespace(view.namespace).void.onError { case e =>
        logger.error(e)(s"Namespace for view '${view.ref}' could not be created.")
      }

    private def compilePipeChain(pipeChain: PipeChain) = PipeChain.compile(pipeChain, registry)

    private def sink(view: ActiveViewDef) = SparqlSink(client, retryStrategy, batchConfig, view.namespace)

    override def destroy(view: ActiveViewDef): IO[Unit] =
      client.deleteNamespace(view.namespace).void
  }

}
