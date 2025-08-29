package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategyConfig}
import ai.senscience.nexus.delta.plugins.blazegraph.SparqlIndexingAction.logger
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.{CurrentActiveViews, IndexingViewDef, SparqlSink}
import ai.senscience.nexus.delta.sdk.indexing.IndexingAction
import ai.senscience.nexus.delta.sdk.model.BaseUri
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
  * To synchronously index a resource in the different SPARQL views of a project
  */
final class SparqlIndexingAction(
    currentViews: CurrentActiveViews,
    pipeChainCompiler: PipeChainCompiler,
    sink: ActiveViewDef => Sink,
    override val timeout: FiniteDuration
) extends IndexingAction {

  override protected def kamonMetricComponent: KamonMetricComponent = KamonMetricComponent("blazegraph-indexing")

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

  override def projections(project: ProjectRef, elem: Elem[GraphResource]): Stream[IO, CompiledProjection] =
    currentViews.stream(project).evalMapFilter { view => compile(view, elem) }
}

object SparqlIndexingAction {

  private val logger = Logger[SparqlIndexingAction]

  def apply(
      currentViews: CurrentActiveViews,
      pipeChainCompiler: PipeChainCompiler,
      client: SparqlClient,
      timeout: FiniteDuration
  )(implicit baseUri: BaseUri): SparqlIndexingAction = {
    val batchConfig   = BatchConfig.individual
    val retryStrategy = RetryStrategyConfig.AlwaysGiveUp
    new SparqlIndexingAction(
      currentViews,
      pipeChainCompiler,
      (v: ActiveViewDef) => SparqlSink(client, retryStrategy, batchConfig, v.namespace),
      timeout
    )
  }

}
