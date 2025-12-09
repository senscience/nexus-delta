package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategyConfig}
import ai.senscience.nexus.delta.plugins.blazegraph.SparqlIndexingAction.logger
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.{CurrentActiveViews, IndexingViewDef, SparqlSink}
import ai.senscience.nexus.delta.sdk.ResourceShifts
import ai.senscience.nexus.delta.sdk.indexing.SyncIndexingAction
import ai.senscience.nexus.delta.sdk.indexing.sync.{SyncIndexingOutcome, SyncIndexingRunner}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, Tag}
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.duration.FiniteDuration

/**
  * To synchronously index a resource in the different SPARQL views of a project
  */
final class SparqlIndexingAction(
    shifts: ResourceShifts,
    currentViews: CurrentActiveViews,
    pipeChainCompiler: PipeChainCompiler,
    sink: ActiveViewDef => Sink,
    timeout: FiniteDuration
)(using Tracer[IO])
    extends SyncIndexingAction {

  override def apply[A](entityType: EntityType)(project: ProjectRef, res: ResourceF[A]): IO[SyncIndexingOutcome] =
    SyncIndexingAction
      .evalMapToElem(entityType)(project, res, shifts.toGraphResource(entityType)(project, _))
      .flatMap { elem =>
        SyncIndexingRunner(
          projections(project, elem),
          timeout
        )
      }
      .surround("sparql-sync-index")

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

  private def projections(project: ProjectRef, elem: Elem[GraphResource]): Stream[IO, CompiledProjection] =
    currentViews.stream(project).evalMapFilter { view => compile(view, elem) }
}

object SparqlIndexingAction {

  private val logger = Logger[SparqlIndexingAction]

  def apply(
      shifts: ResourceShifts,
      currentViews: CurrentActiveViews,
      pipeChainCompiler: PipeChainCompiler,
      client: SparqlClient,
      timeout: FiniteDuration
  )(using BaseUri, Tracer[IO]): SparqlIndexingAction = {
    val batchConfig   = BatchConfig.individual
    val retryStrategy = RetryStrategyConfig.AlwaysGiveUp
    new SparqlIndexingAction(
      shifts,
      currentViews,
      pipeChainCompiler,
      (v: ActiveViewDef) => SparqlSink(client, retryStrategy, batchConfig, v.namespace),
      timeout
    )
  }

}
