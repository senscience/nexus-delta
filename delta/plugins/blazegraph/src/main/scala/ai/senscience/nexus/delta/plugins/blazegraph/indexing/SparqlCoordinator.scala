package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.cache.LocalCache
import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import cats.effect.IO
import cats.syntax.all.*

sealed trait SparqlCoordinator

object SparqlCoordinator {

  /** If indexing is disabled we can only log */
  final private case object Noop extends SparqlCoordinator {
    def log: IO[Unit] =
      logger.info("Sparql indexing has been disabled via config")
  }

  /**
    * Coordinates the lifecycle of Sparql views as projections
    * @param fetchViews
    *   stream of indexing views
    * @param projectionLifeCycle
    *   to provide the data feeding the Sparql projections
    * @param cache
    *   a cache of the current running views
    * @param supervisor
    *   the general supervisor
    */
  final private class Active(
      fetchViews: Offset => SuccessElemStream[IndexingViewDef],
      projectionLifeCycle: SparqlProjectionLifeCycle,
      cache: LocalCache[ViewRef, ActiveViewDef],
      supervisor: Supervisor
  ) extends SparqlCoordinator {

    def run(offset: Offset): ElemStream[Unit] = {
      fetchViews(offset)
        .pauseWhen(projectionLifeCycle.failing)
        .evalMap { elem =>
          elem
            .traverse { processView }
            .recover {
              // If the current view does not translate to a projection then we mark it as failed and move along
              case p: ProjectionErr => elem.failed(p)
            }
            .map(_.void)
        }
    }

    private def processView(v: IndexingViewDef) =
      cache.get(v.ref).flatMap { cachedView =>
        (cachedView, v) match {
          case (Some(cached), active: ActiveViewDef) if cached.projection == active.projection =>
            cache.put(active.ref, active) >>
              logger.info(s"Index ${active.projection} already exists and will not be recreated.")
          case (cached, active: ActiveViewDef)                                                 =>
            def init = projectionLifeCycle.init(active) >> cache.put(active.ref, active)
            for {
              projection <- projectionLifeCycle.compile(active)
              _          <- cleanupRunning(cached, active.ref)
              _          <- supervisor.run(projection, init)
            } yield ()
          case (cached, deprecated: DeprecatedViewDef)                                         =>
            cleanupRunning(cached, deprecated.ref)
        }
      }

    private def cleanupRunning(cached: Option[ActiveViewDef], ref: ViewRef): IO[Unit] =
      cached match {
        case Some(v) =>
          def clear =
            logger.info(s"View '$ref' has been updated or deprecated, cleaning up the current one.") >>
              projectionLifeCycle.destroy(v) >>
              cache.remove(v.ref)
          supervisor.destroy(v.projection, clear).void
        case None    =>
          logger.debug(s"View '${ref.project}/${ref.viewId}' is not referenced yet, cleaning is aborted.")
      }

  }

  val metadata: ProjectionMetadata = ProjectionMetadata("system", "sparql-coordinator", None, None)
  private val logger               = Logger[SparqlCoordinator]

  def apply(
      views: BlazegraphViews,
      projectionLifeCycle: SparqlProjectionLifeCycle,
      supervisor: Supervisor,
      indexingEnabled: Boolean
  ): IO[SparqlCoordinator] =
    if (indexingEnabled) {
      apply(views.indexingViews, projectionLifeCycle, supervisor)
    } else {
      Noop.log.as(Noop)
    }

  def apply(
      fetchViews: Offset => SuccessElemStream[IndexingViewDef],
      projectionLifeCycle: SparqlProjectionLifeCycle,
      supervisor: Supervisor
  ): IO[SparqlCoordinator] =
    LocalCache[ViewRef, ActiveViewDef]().flatMap { cache =>
      val coordinator = new Active(fetchViews, projectionLifeCycle, cache, supervisor)
      supervisor
        .run(
          CompiledProjection.fromStream(
            metadata,
            ExecutionStrategy.EveryNode,
            coordinator.run
          )
        )
        .as(coordinator)
    }
}
