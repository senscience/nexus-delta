package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchIndexingAction.logger
import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.elasticsearch.indexing.{CurrentActiveViews, ElasticSearchSink, IndexingViewDef}
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.indexing.IndexingAction
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tag}
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

/**
  * To synchronously index a resource in the different Elasticsearch views of a project
  */
final class ElasticSearchIndexingAction(
    currentViews: CurrentActiveViews,
    pipeChainCompiler: PipeChainCompiler,
    sink: ActiveViewDef => Sink,
    override val timeout: FiniteDuration
)(implicit cr: RemoteContextResolution)
    extends IndexingAction {

  override protected def kamonMetricComponent: KamonMetricComponent = KamonMetricComponent(
    "elasticsearch-custom-indexing"
  )

  private def compile(view: ActiveViewDef, elem: Elem[GraphResource]): IO[Option[CompiledProjection]] =
    Option.when(view.selectFilter.tag == Tag.latest)(view).flatTraverse { v =>
      // Synchronous indexing only applies to views that index the latest version
      IndexingViewDef
        .compile(v, pipeChainCompiler, Stream(elem), sink(v))
        .redeemWith(
          err => logger.error(err)(s"View '$view' could not be compiled.").as(None),
          IO.some
        )
    }

  def projections(project: ProjectRef, elem: Elem[GraphResource]): Stream[IO, CompiledProjection] =
    currentViews.stream(project).evalMapFilter { view => compile(view, elem) }
}
object ElasticSearchIndexingAction {

  private val logger = Logger[ElasticSearchIndexingAction]

  def apply(
      currentViews: CurrentActiveViews,
      pipeChainCompiler: PipeChainCompiler,
      client: ElasticSearchClient,
      timeout: FiniteDuration,
      syncIndexingRefresh: Refresh
  )(implicit cr: RemoteContextResolution): ElasticSearchIndexingAction = {
    val batchConfig = BatchConfig.individual
    new ElasticSearchIndexingAction(
      currentViews,
      pipeChainCompiler,
      (v: ActiveViewDef) => ElasticSearchSink.states(client, batchConfig, v.index, syncIndexingRefresh),
      timeout
    )
  }
}
