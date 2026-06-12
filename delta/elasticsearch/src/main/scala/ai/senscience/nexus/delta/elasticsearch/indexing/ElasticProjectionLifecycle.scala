package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.config.ElasticSearchViewsConfig
import ai.senscience.nexus.delta.elasticsearch.indexing.ElasticSearchRunningStore.ElasticRunningView
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, PipeChainCompiler}
import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer

/**
  * Encapsulates the compilation, the index lifecycle (creation/deletion) and the durable bookkeeping of an
  * Elasticsearch view projection, so that the [[ElasticSearchCoordinator]] only deals with the supervision flow.
  */
trait ElasticProjectionLifecycle {

  def compile(view: ActiveViewDef): IO[CompiledProjection]

  /**
    * Creates the index for the view and records the running view (the supervisor-gated start hook).
    */
  def init(view: ActiveViewDef): IO[Unit]

  /**
    * The running views recorded for the given view, used to reconcile and clean up old revisions.
    */
  def recorded(ref: ViewRef): IO[List[ElasticRunningView]]

  /**
    * Deletes the index of a recorded running view and forgets it (the supervisor-gated cleanup hook).
    */
  def destroy(view: ElasticRunningView): IO[Unit]

}

object ElasticProjectionLifecycle {

  private val logger = Logger[ElasticProjectionLifecycle]

  def apply(
      graphStream: GraphResourceStream,
      pipeChainCompiler: PipeChainCompiler,
      client: ElasticSearchClient,
      config: ElasticSearchViewsConfig,
      store: ElasticSearchRunningStore
  )(using RemoteContextResolution, Tracer[IO]): ElasticProjectionLifecycle = {
    def sink(view: ActiveViewDef): Sink =
      ElasticSearchSink.states(client, config.retryStrategy, config.batch, view.index, Refresh.False)
    apply(
      view => IndexingViewDef.compile(view, pipeChainCompiler, graphStream, sink(view)),
      view =>
        client
          .createIndex(view.index, view.indexDef)
          .onError { case e =>
            logger.error(e)(s"Index for view '${view.ref.project}/${view.ref.viewId}' could not be created.")
          }
          .void,
      view => client.deleteIndex(ElasticSearchViews.index(view.uuid, view.indexingRev, config.prefix)).void,
      store
    )
  }

  /**
    * Builds a lifecycle from the underlying operations directly, decoupled from the Elasticsearch client. Mostly useful
    * to test the store bookkeeping without a running Elasticsearch.
    *
    * @param compileView
    *   compiles the view into a projection
    * @param createIndex
    *   creates the index of the view
    * @param deleteIndex
    *   deletes the index of a recorded running view
    * @param store
    *   the running views store
    */
  def apply(
      compileView: ActiveViewDef => IO[CompiledProjection],
      createIndex: ActiveViewDef => IO[Unit],
      deleteIndex: ElasticRunningView => IO[Unit],
      store: ElasticSearchRunningStore
  ): ElasticProjectionLifecycle = new ElasticProjectionLifecycle {

    override def compile(view: ActiveViewDef): IO[CompiledProjection] =
      compileView(view)

    override def init(view: ActiveViewDef): IO[Unit] =
      createIndex(view) >> store.save(ElasticRunningView(view.ref, view.indexingRev, view.uuid))

    override def recorded(ref: ViewRef): IO[List[ElasticRunningView]] =
      store.list(ref)

    override def destroy(view: ElasticRunningView): IO[Unit] =
      deleteIndex(view) >> store.delete(view.ref, view.indexingRev)
  }

}
