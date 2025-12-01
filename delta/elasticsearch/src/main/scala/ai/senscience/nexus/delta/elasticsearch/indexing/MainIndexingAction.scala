package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.elasticsearch.indexing.MainIndexingCoordinator.mainIndexingPipeline
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.indexing.IndexingAction
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import fs2.Stream
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.duration.FiniteDuration

final class MainIndexingAction(sink: Sink, override val timeout: FiniteDuration)(using
    RemoteContextResolution,
    Tracer[IO]
) extends IndexingAction {

  private def compile(project: ProjectRef, elem: Elem[GraphResource]) =
    CompiledProjection.compile(
      mainIndexingProjectionMetadata(project),
      ExecutionStrategy.TransientSingleNode,
      Source(_ => Stream(elem)),
      mainIndexingPipeline,
      sink
    )

  override def projections(project: ProjectRef, elem: Elem[GraphResource]): Stream[IO, CompiledProjection] =
    Stream.fromEither[IO](compile(project, elem))

  override def tracer: Tracer[IO] = Tracer[IO]
}

object MainIndexingAction {
  def apply(
      client: ElasticSearchClient,
      config: MainIndexConfig,
      timeout: FiniteDuration,
      syncIndexingRefresh: Refresh
  )(using RemoteContextResolution, Tracer[IO]): MainIndexingAction = {
    val batchConfig = BatchConfig.individual
    new MainIndexingAction(
      ElasticSearchSink.mainIndexing(client, batchConfig, config.index, syncIndexingRefresh),
      timeout
    )
  }
}
