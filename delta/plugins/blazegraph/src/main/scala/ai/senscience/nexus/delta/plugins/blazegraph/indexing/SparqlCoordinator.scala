package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.SparqlRunningStore.SparqlRunningView
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import cats.effect.IO
import cats.syntax.all.*

sealed trait SparqlCoordinator

object SparqlCoordinator {

  /** If indexing is disabled we can only log */
  private case object Noop extends SparqlCoordinator {
    def log: IO[Unit] =
      logger.info("Sparql indexing has been disabled via config")
  }

  /**
    * Coordinates the lifecycle of Sparql views as projections
    * @param fetchViews
    *   stream of indexing views
    * @param projectionLifeCycle
    *   to compile the projections, manage the underlying namespaces and record the running views
    * @param supervisor
    *   the general supervisor
    * @param resumer
    *   resumes passivated views as activations come in, and tells whether a project is active
    */
  final private class Active(
      fetchViews: Offset => SuccessElemStream[IndexingViewDef],
      projectionLifeCycle: SparqlProjectionLifeCycle,
      supervisor: Supervisor,
      resumer: SparqlProjectionResumer
  ) extends SparqlCoordinator {

    def run(offset: Offset): ElemStream[Unit] = {
      val processViews = fetchViews(offset).evalMap { elem =>
        elem
          .traverse(processView)
          .recoverWith {
            // If the current view does not translate to a projection then we mark it as failed and move along
            case p: ProjectionErr =>
              val message = s"Projection for '${elem.project}/${elem.id}' failed for a compilation problem."
              logger.error(p)(message).as(elem.failed(p))
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
            // Clean up any revision other than the current one (old namespaces/projections); each cleanup self-gates to
            // the node owning that revision.
            _        <- recorded.filterNot(_.indexingRev == active.indexingRev).traverse_(cleanup)
            // Start the current revision unless it is already materialized; only if its project is active.
            _        <- IO.unlessA(recorded.exists(_.indexingRev == active.indexingRev))(startIfActive(active))
          } yield ()
        case deprecated: DeprecatedViewDef =>
          // A deprecated view has no surviving projection: clean up all of them. The def carries the current revision,
          // so the immutable default view (never recorded in the store) is covered; the store provides any other
          // lingering revisions.
          val current = SparqlRunningView(deprecated.ref, deprecated.indexingRev, deprecated.uuid)
          projectionLifeCycle.recorded(deprecated.ref).flatMap { recorded =>
            (current :: recorded.filterNot(_.indexingRev == deprecated.indexingRev)).traverse_(cleanup)
          }
      }

    private def startIfActive(view: ActiveViewDef): IO[Unit] =
      resumer.isActive(view.ref.project).flatMap {
        case true  => start(view)
        case false => logger.debug(s"View '${view.ref}' is not started as its project is not active.")
      }

    /** Compiles and runs the view, creating its namespace and recording the running view as part of the init. */
    private def start(view: ActiveViewDef): IO[Unit] =
      projectionLifeCycle.compile(view).flatMap { projection =>
        supervisor.run(projection, projectionLifeCycle.init(view)).void
      }

    private def resumeView(view: ActiveViewDef): IO[Unit] =
      logger.info(s"Resuming projection '${view.projection}' for active project '${view.ref.project}'.") >> start(view)

    /**
      * Stops and removes the projection for a recorded view revision and deletes its namespace. The namespace deletion
      * and row removal run in `destroy`'s finalizer, so they are gated to the node that owns that revision.
      */
    private def cleanup(view: SparqlRunningView): IO[Unit] = {
      val compiled = CompiledProjection.noop(view.metadata, ExecutionStrategy.PersistentSingleNode)
      val clear    =
        logger.info(s"Cleaning up the previous revision '${view.projection}' of view '${view.ref}'.") >>
          projectionLifeCycle.destroy(view)
      supervisor.destroy(compiled, clear).void
    }

  }

  val metadata: ProjectionMetadata = ProjectionMetadata("system", "sparql-coordinator", None, None)
  private val logger               = Logger[SparqlCoordinator]

  private def coordinatorProjection(coordinator: Active) =
    CompiledProjection.fromStream(
      metadata,
      ExecutionStrategy.EveryNode,
      offset => coordinator.run(offset)
    )

  def apply(
      views: BlazegraphViews,
      projectionLifeCycle: SparqlProjectionLifeCycle,
      supervisor: Supervisor,
      resumer: SparqlProjectionResumer,
      indexingEnabled: Boolean
  ): IO[SparqlCoordinator] =
    if indexingEnabled then {
      apply(views.indexingViews, projectionLifeCycle, supervisor, resumer)
    } else {
      Noop.log.as(Noop)
    }

  def apply(
      fetchViews: Offset => SuccessElemStream[IndexingViewDef],
      projectionLifeCycle: SparqlProjectionLifeCycle,
      supervisor: Supervisor,
      resumer: SparqlProjectionResumer
  ): IO[SparqlCoordinator] = {
    val coordinator = new Active(fetchViews, projectionLifeCycle, supervisor, resumer)
    supervisor.run(coordinatorProjection(coordinator)).as(coordinator)
  }
}
