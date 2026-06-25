package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.indexing.ElasticSearchRunningStore.ElasticRunningView
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.elasticsearch.query.ElasticSearchClientError.ElasticsearchCreateIndexError
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import cats.effect.IO
import cats.syntax.all.*

sealed trait ElasticSearchCoordinator

object ElasticSearchCoordinator {

  /** If indexing is disabled we can only log */
  private case object Noop extends ElasticSearchCoordinator {
    def log: IO[Unit] = logger.info("Elasticsearch indexing has been disabled via config")
  }

  /**
    * Coordinates the lifecycle of Elasticsearch views as projections
    * @param fetchViews
    *   stream of indexing views
    * @param projectionLifeCycle
    *   to compile the projections, manage the underlying indices and record the running views
    * @param supervisor
    *   the general supervisor
    * @param resumer
    *   resumes passivated views as activations come in, and tells whether a project is active
    */
  final private class Active(
      fetchViews: Offset => SuccessElemStream[IndexingViewDef],
      projectionLifeCycle: ElasticProjectionLifecycle,
      supervisor: Supervisor,
      resumer: ElasticProjectionResumer
  ) extends ElasticSearchCoordinator {

    def run(offset: Offset): ElemStream[Unit] = {
      val processViews = fetchViews(offset).evalMap { elem =>
        elem
          .traverse(processView)
          .recoverWith {
            // If the current view does not translate to a projection or if there is a problem
            // with the mapping / setting then we mark it as failed and move along
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
      processViews.concurrently(resumer.run(resumeView))
    }

    private def processView(v: IndexingViewDef): IO[Unit] =
      v match {
        case active: ActiveViewDef         =>
          for {
            recorded <- projectionLifeCycle.recorded(active.ref)
            // Clean up any revision other than the current one (old indices/projections); each cleanup self-gates to
            // the node owning that revision.
            _        <- recorded.filterNot(_.indexingRev == active.indexingRev).traverse(cleanup)
            // Start the current revision unless it is already materialized.
            _        <- IO.unlessA(recorded.exists(_.indexingRev == active.indexingRev))(start(active))
          } yield ()
        case deprecated: DeprecatedViewDef =>
          // A deprecated view has no surviving projection: clean up all of them.
          projectionLifeCycle.recorded(deprecated.ref).flatMap(_.traverse_(cleanup))
      }

    /** Compiles and runs the view, creating its index and recording the running view as part of the init. */
    private def start(view: ActiveViewDef): IO[Unit] =
      projectionLifeCycle.compile(view).flatMap { projection =>
        supervisor.run(projection, projectionLifeCycle.init(view)).void
      }

    private def resumeView(view: ActiveViewDef): IO[Unit] =
      logger.info(s"Resuming projection '${view.projection}' for active project '${view.ref.project}'.") >> start(view)

    /**
      * Stops and removes the projection for a recorded view revision and deletes its index. The index deletion and row
      * removal run in `destroy`'s finalizer, so they are gated to the node that owns that revision.
      */
    private def cleanup(view: ElasticRunningView): IO[Unit] = {
      val compiled = CompiledProjection.noop(view.metadata, ExecutionStrategy.PersistentSingleNode)
      val clear    =
        logger.info(s"Cleaning up the previous revision '${view.projection}' of view '${view.ref}'.") >>
          projectionLifeCycle.destroy(view)
      supervisor.destroy(compiled, clear).void
    }

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
      projectionLifeCycle: ElasticProjectionLifecycle,
      supervisor: Supervisor,
      resumer: ElasticProjectionResumer,
      indexingEnabled: Boolean
  ): IO[ElasticSearchCoordinator] =
    if indexingEnabled then {
      apply(views.indexingViews, projectionLifeCycle, supervisor, resumer)
    } else {
      Noop.log.as(Noop)
    }

  def apply(
      fetchViews: Offset => SuccessElemStream[IndexingViewDef],
      projectionLifeCycle: ElasticProjectionLifecycle,
      supervisor: Supervisor,
      resumer: ElasticProjectionResumer
  ): IO[ElasticSearchCoordinator] = {
    val coordinator = new Active(fetchViews, projectionLifeCycle, supervisor, resumer)
    supervisor.run(coordinatorProjection(coordinator)).as(coordinator)
  }
}
