package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import MainIndexingCoordinator.mainIndexingPipeline
import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.IndexingAction
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset.Start
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import fs2.Stream

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

final class MainIndexingAction(sink: Sink, override val timeout: FiniteDuration)(implicit
    cr: RemoteContextResolution
) extends IndexingAction {

  override protected def kamonMetricComponent: KamonMetricComponent = KamonMetricComponent(
    "elasticsearch-main-indexing"
  )

  private def compile(project: ProjectRef, elem: Elem[GraphResource]) =
    CompiledProjection.compile(
      mainIndexingProjectionMetadata(project),
      ExecutionStrategy.TransientSingleNode,
      Source(_ => Stream(elem)),
      mainIndexingPipeline,
      sink
    )

  override def projections(project: ProjectRef, elem: Elem[GraphResource]): ElemStream[CompiledProjection] = {
    // TODO: get rid of elem here
    val entityType = EntityType("main-indexing")
    Stream.fromEither[IO](
      compile(project, elem).map { projection =>
        SuccessElem(entityType, mainIndexingId, project, Instant.EPOCH, Start, projection, 1)
      }
    )
  }
}

object MainIndexingAction {
  def apply(
      client: ElasticSearchClient,
      config: MainIndexConfig,
      timeout: FiniteDuration,
      syncIndexingRefresh: Refresh
  )(implicit cr: RemoteContextResolution): MainIndexingAction = {
    val batchConfig = BatchConfig.individual
    new MainIndexingAction(
      ElasticSearchSink.mainIndexing(client, batchConfig, config.index, syncIndexingRefresh),
      timeout
    )
  }
}
