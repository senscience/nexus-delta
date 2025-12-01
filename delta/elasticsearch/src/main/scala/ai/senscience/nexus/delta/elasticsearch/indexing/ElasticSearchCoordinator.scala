package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.config.ElasticSearchViewsConfig
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.elasticsearch.query.ElasticSearchClientError.ElasticsearchCreateIndexError
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.cache.LocalCache
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.trace.Tracer

sealed trait ElasticSearchCoordinator

object ElasticSearchCoordinator {

  /** If indexing is disabled we can only log */
  private case object Noop extends ElasticSearchCoordinator {
    def log: IO[Unit] =
      logger.info("Elasticsearch indexing has been disabled via config")

  }

  /**
    * Coordinates the lifecycle of Elasticsearch views as projections
    * @param fetchViews
    *   stream of indexing views
    * @param graphStream
    *   to provide the data feeding the Elasticsearch projections
    * @param pipeChainCompiler
    *   to compile and validate pipechains before running them
    * @param cache
    *   a cache of the current running views
    * @param supervisor
    *   the general supervisor
    */
  final private class Active(
      fetchViews: Offset => SuccessElemStream[IndexingViewDef],
      graphStream: GraphResourceStream,
      pipeChainCompiler: PipeChainCompiler,
      cache: LocalCache[ViewRef, ActiveViewDef],
      supervisor: Supervisor,
      sink: ActiveViewDef => Sink,
      createIndex: ActiveViewDef => IO[Unit],
      deleteIndex: ActiveViewDef => IO[Unit]
  )(using RemoteContextResolution)
      extends ElasticSearchCoordinator {

    def run(offset: Offset): ElemStream[Unit] = {
      fetchViews(offset).evalMap { elem =>
        elem
          .traverse { v =>
            cache
              .get(v.ref)
              .flatMap { cachedView =>
                (cachedView, v) match {
                  case (Some(cached), active: ActiveViewDef) if cached.index == active.index =>
                    for {
                      _ <- cache.put(active.ref, active)
                      _ <- logger.info(s"Index ${active.index} already exists and will not be recreated.")
                    } yield ()
                  case (cached, active: ActiveViewDef)                                       =>
                    val init = createIndex(active) >> cache.put(active.ref, active)
                    compile(active)
                      .flatMap { projection =>
                        cleanupCurrent(cached, active.ref) >>
                          supervisor.run(projection, init)
                      }
                  case (cached, deprecated: DeprecatedViewDef)                               =>
                    cleanupCurrent(cached, deprecated.ref)
                }
              }
          }
          .recoverWith {
            // If the current view does not translate to a projection or if there is a problem
            // with the mapping with the mapping / setting then we mark it as failed and move along
            case p: ProjectionErr                 =>
              val message = s"Projection for '${elem.project}/${elem.id}' failed for a compilation problem."
              logger.error(p)(message).as(elem.failed(p))
            case e: ElasticsearchCreateIndexError =>
              val message =
                s"Projection for '${elem.project}/${elem.id}' failed at index creation. Please check its mapping/settings."
              logger.error(e)(message).as(elem.failed(e))
          }
          .map(_.void)
      }
    }

    private def cleanupCurrent(cached: Option[ActiveViewDef], ref: ViewRef): IO[Unit] =
      cached match {
        case Some(v) =>
          supervisor
            .destroy(
              v.projection,
              for {
                _ <-
                  logger.info(
                    s"View '${ref.project}/${ref.viewId}' has been updated or deprecated, cleaning up the current one."
                  )
                _ <- deleteIndex(v)
                _ <- cache.remove(v.ref)
              } yield ()
            )
            .void
        case None    =>
          logger.debug(s"View '${ref.project}/${ref.viewId}' is not referenced yet, cleaning is aborted.")
      }

    private def compile(active: ActiveViewDef): IO[CompiledProjection] =
      IndexingViewDef.compile(active, pipeChainCompiler, graphStream, sink(active))

  }

  val metadata: ProjectionMetadata = ProjectionMetadata("system", "elasticsearch-coordinator", None, None)
  private val logger               = Logger[ElasticSearchCoordinator]

  private def coordinatorProjection(coordinator: Active) =
    CompiledProjection.fromStream(
      metadata,
      ExecutionStrategy.EveryNode,
      offset => coordinator.run(offset)
    )

  def apply(
      views: ElasticSearchViews,
      graphStream: GraphResourceStream,
      pipeChainCompiler: PipeChainCompiler,
      supervisor: Supervisor,
      client: ElasticSearchClient,
      config: ElasticSearchViewsConfig
  )(using RemoteContextResolution, Tracer[IO]): IO[ElasticSearchCoordinator] = {
    if config.indexingEnabled then {
      apply(
        views.indexingViews,
        graphStream,
        pipeChainCompiler,
        supervisor,
        (v: ActiveViewDef) => ElasticSearchSink.states(client, config.batch, v.index, Refresh.False),
        (v: ActiveViewDef) =>
          client
            .createIndex(v.index, Some(v.mapping), Some(v.settings))
            .onError { case e =>
              logger.error(e)(s"Index for view '${v.ref.project}/${v.ref.viewId}' could not be created.")
            }
            .void,
        (v: ActiveViewDef) => client.deleteIndex(v.index).void
      )
    } else {
      Noop.log.as(Noop)
    }
  }

  def apply(
      fetchViews: Offset => SuccessElemStream[IndexingViewDef],
      graphStream: GraphResourceStream,
      pipeChainCompiler: PipeChainCompiler,
      supervisor: Supervisor,
      sink: ActiveViewDef => Sink,
      createIndex: ActiveViewDef => IO[Unit],
      deleteIndex: ActiveViewDef => IO[Unit]
  )(using RemoteContextResolution): IO[ElasticSearchCoordinator] =
    for {
      cache      <- LocalCache[ViewRef, ActiveViewDef]()
      coordinator = new Active(
                      fetchViews,
                      graphStream,
                      pipeChainCompiler,
                      cache,
                      supervisor,
                      sink,
                      createIndex,
                      deleteIndex
                    )
      _          <- supervisor.run(coordinatorProjection(coordinator))
    } yield coordinator
}
