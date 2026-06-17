package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategyConfig}
import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.SparqlRunningStore.SparqlRunningView
import ai.senscience.nexus.delta.plugins.blazegraph.model.defaultViewId
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, PipeChainCompiler}
import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer

trait SparqlProjectionLifeCycle {

  def compile(view: ActiveViewDef): IO[CompiledProjection]

  /**
    * Creates the namespace for the view and records the running view (the supervisor-gated start hook). The default
    * view is immutable, so it is never recorded.
    */
  def init(view: ActiveViewDef): IO[Unit]

  /**
    * The running views recorded for the given view, used to reconcile and clean up old revisions. The default view is
    * never recorded, so this is always empty for it.
    */
  def recorded(ref: ViewRef): IO[List[SparqlRunningView]]

  /**
    * Deletes the namespace of a recorded running view and forgets it (the supervisor-gated cleanup hook).
    */
  def destroy(view: SparqlRunningView): IO[Unit]

}

object SparqlProjectionLifeCycle {

  private val logger = Logger[SparqlProjectionLifeCycle]

  def apply(
      graphStream: GraphResourceStream,
      pipeChainCompiler: PipeChainCompiler,
      client: SparqlClient,
      retryStrategy: RetryStrategyConfig,
      batchConfig: BatchConfig,
      prefix: String,
      store: SparqlRunningStore
  )(using BaseUri, Tracer[IO]): SparqlProjectionLifeCycle = {
    def sink(view: ActiveViewDef) = SparqlSink(client, retryStrategy, batchConfig, view.namespace)
    apply(
      view =>
        IndexingViewDef.compile(
          view,
          pipeChainCompiler,
          graphStream.continuous(view.ref.project, view.selectFilter, _),
          sink(view)
        ),
      view =>
        client.createNamespace(view.namespace).void.onError { case e =>
          logger.error(e)(s"Namespace for view '${view.ref}' could not be created.")
        },
      view => client.deleteNamespace(BlazegraphViews.namespace(view.uuid, view.indexingRev, prefix)).void,
      store
    )
  }

  /**
    * Builds a lifecycle from the underlying operations directly, decoupled from the Sparql client. Mostly useful to
    * test the store bookkeeping (including the default view never being recorded) without a running Blazegraph.
    *
    * @param compileView
    *   compiles the view into a projection
    * @param createNamespace
    *   creates the namespace of the view
    * @param deleteNamespace
    *   deletes the namespace of a recorded running view
    * @param store
    *   the running views store
    */
  def apply(
      compileView: ActiveViewDef => IO[CompiledProjection],
      createNamespace: ActiveViewDef => IO[Unit],
      deleteNamespace: SparqlRunningView => IO[Unit],
      store: SparqlRunningStore
  ): SparqlProjectionLifeCycle = new SparqlProjectionLifeCycle {

    override def compile(view: ActiveViewDef): IO[CompiledProjection] =
      compileView(view)

    override def init(view: ActiveViewDef): IO[Unit] =
      // The default view is immutable, so it is never recorded.
      createNamespace(view) >> IO.unlessA(isDefault(view.ref))(
        store.save(SparqlRunningView(view.ref, view.indexingRev, view.uuid))
      )

    override def recorded(ref: ViewRef): IO[List[SparqlRunningView]] =
      // The default view is immutable and never recorded: there is no previous revision to list.
      if isDefault(ref) then IO.pure(List.empty) else store.list(ref)

    override def destroy(view: SparqlRunningView): IO[Unit] =
      deleteNamespace(view) >> store.delete(view.ref, view.indexingRev)

    private def isDefault(ref: ViewRef): Boolean = ref.viewId == defaultViewId
  }

}
