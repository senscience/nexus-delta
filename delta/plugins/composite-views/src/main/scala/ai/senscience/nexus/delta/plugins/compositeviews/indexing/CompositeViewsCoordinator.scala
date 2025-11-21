package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.cache.LocalCache
import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViews
import ai.senscience.nexus.delta.plugins.compositeviews.config.CompositeViewsConfig
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import cats.effect.IO
import cats.syntax.all.*

sealed trait CompositeViewsCoordinator

object CompositeViewsCoordinator {

  /** If indexing is disabled we can only log */
  private case object Noop extends CompositeViewsCoordinator {
    def log: IO[Unit] = logger.info("Composite Views indexing has been disabled via config")
  }

  /**
    * Coordinates the lifecycle of composite views as projections
    *
    * @param fetchViews
    *   to fetch the composite views
    * @param cache
    *   a cache of the current running views
    * @param supervisor
    *   the general supervisor
    * @param lifecycle
    *   defines how to init, run and destroy the projection related to the view
    */
  final private class Active(
      fetchViews: Offset => ElemStream[CompositeViewDef],
      cache: LocalCache[ViewRef, ActiveViewDef],
      supervisor: Supervisor,
      lifecycle: CompositeProjectionLifeCycle
  ) extends CompositeViewsCoordinator {

    def run(offset: Offset): ElemStream[Unit] = {
      fetchViews(offset).evalMap {
        _.traverse {
          case active: ActiveViewDef =>
            for {
              // Compiling and validating the view
              projection <- lifecycle.build(active)
              // Stopping the previous version
              _          <- cleanupCurrent(cache, active, triggerDestroy)
              // Init, register and run the new version
              _          <- supervisor.run(
                              projection,
                              for {
                                _ <- lifecycle.init(active)
                                _ <- cache.put(active.ref, active)
                              } yield ()
                            )
            } yield ()
          case d: DeprecatedViewDef  => cleanupCurrent(cache, d, triggerDestroy)
        }
      }
    }

    private def triggerDestroy(prev: ActiveViewDef, next: CompositeViewDef) =
      supervisor
        .destroy(
          prev.projection,
          for {
            _ <- lifecycle.destroyOnIndexingChange(prev, next)
            _ <- cache.remove(prev.ref)
          } yield ()
        )
        .void

  }

  private val metadata: ProjectionMetadata = ProjectionMetadata("system", "composite-views-coordinator", None, None)
  private val logger                       = Logger[CompositeViewsCoordinator]

  def cleanupCurrent(
      cache: LocalCache[ViewRef, ActiveViewDef],
      next: CompositeViewDef,
      triggerDestroy: (ActiveViewDef, CompositeViewDef) => IO[Unit]
  ): IO[Unit] = {
    val ref = next.ref
    cache.get(ref).flatMap {
      case Some(cached) => triggerDestroy(cached, next)
      case None         => logger.debug(s"View '${ref.project}/${ref.viewId}' is not referenced yet, cleaning is aborted.")
    }
  }

  def apply(
      compositeViews: CompositeViews,
      supervisor: Supervisor,
      builder: CompositeProjectionLifeCycle,
      config: CompositeViewsConfig
  ): IO[CompositeViewsCoordinator] = {
    if config.indexingEnabled then {
      for {
        cache      <- LocalCache[ViewRef, ActiveViewDef]()
        coordinator = new Active(
                        compositeViews.views,
                        cache,
                        supervisor,
                        builder
                      )
        _          <- supervisor.run(
                        CompiledProjection.fromStream(
                          metadata,
                          ExecutionStrategy.EveryNode,
                          coordinator.run
                        )(using ProjectionBackpressure.Noop)
                      )
      } yield coordinator
    } else {
      Noop.log.as(Noop)
    }
  }

}
