package ai.senscience.nexus.delta.elasticsearch.query

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, ElasticSearchRequest}
import ai.senscience.nexus.delta.elasticsearch.configured.ConfiguredIndexingConfig
import ai.senscience.nexus.delta.elasticsearch.configured.ConfiguredIndexingConfig.Enabled
import ai.senscience.nexus.delta.elasticsearch.query.ConfiguredIndexQuery.Target
import ai.senscience.nexus.delta.sdk.syntax.surround
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import cats.effect.IO
import cats.effect.kernel.Clock
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.duration.{Duration, FiniteDuration}

trait ConfiguredIndexQuery {

  /**
    * Search on the configured indices the given query
    */
  def search(project: ProjectRef, target: Target, request: ElasticSearchRequest): IO[Json]

  /**
    * Refresh the configured indices Note that the refresh is only possible at the rate defined in the configuration
    */
  def refresh: IO[Unit]

}

object ConfiguredIndexQuery {

  sealed trait Target

  object Target {
    case object All extends Target

    final case class Single(value: Label) extends Target
  }

  case object Noop extends ConfiguredIndexQuery {

    private val disabled = IO.raiseError(new IllegalStateException("Configured indexing is disabled."))

    override def search(project: ProjectRef, target: Target, request: ElasticSearchRequest): IO[Json] = disabled
    override def refresh: IO[Unit]                                                                    = disabled
  }

  final class Active(
      client: ElasticSearchClient,
      config: ConfiguredIndexingConfig.Enabled,
      lastRefresh: AtomicCell[IO, FiniteDuration],
      clock: Clock[IO]
  )(using Tracer[IO])
      extends ConfiguredIndexQuery {

    private val indexMap = config.indices.foldLeft(Map.empty[Label, String]) { case (acc, index) =>
      acc + (index.index -> index.prefixedIndex(config.prefix).value)
    }

    override def search(project: ProjectRef, target: Target, request: ElasticSearchRequest): IO[Json] = {
      val indices = target match {
        case Target.All           => indexMap.values.toSet
        case Target.Single(value) => indexMap.get(value).toSet
      }
      client.search(FilterByProject(project, request), indices)
    }.surround("configuredSearch")

    override def refresh: IO[Unit] =
      clock.realTime
        .flatTap { now =>
          lastRefresh.evalUpdate { current =>
            IO.whenA(now > current) {
              IO.sleep(now.minus(current).min(config.maxRefreshPeriod)) >>
                config.indices.parUnorderedTraverse { index =>
                  client.refresh(index.prefixedIndex(config.prefix))
                }.void
            }.as(now)
          }
        }
        .void
        .surround("configuredRefresh")
  }

  def apply(client: ElasticSearchClient, config: ConfiguredIndexingConfig, clock: Clock[IO])(using
      Tracer[IO]
  ): IO[ConfiguredIndexQuery] =
    config match {
      case ConfiguredIndexingConfig.Disabled        =>
        IO.pure(Noop)
      case config: ConfiguredIndexingConfig.Enabled =>
        AtomicCell[IO].of(Duration.Zero).map { lastRefresh =>
          new Active(client, config, lastRefresh, clock)
        }

    }
}
