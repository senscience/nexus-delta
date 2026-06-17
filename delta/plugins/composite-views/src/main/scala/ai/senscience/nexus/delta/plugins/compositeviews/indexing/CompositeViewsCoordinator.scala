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
    *   a cache of the current running views, used as cleanup memory for the fine-grained per-projection diff
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

    def run(offset: Offset): ElemStream[Unit] =
      fetchViews(offset).evalMap {
        _.traverse {
          case active: ActiveViewDef =>
            // Stop the previous version (if any), then (re)start the new one.
            cleanupCurrent(active, triggerDestroy) >> start(active)
          case d: DeprecatedViewDef  => cleanupCurrent(d, triggerDestroy)
        }
      }

    /**
      * Compiles, registers and runs the view, recording it in the cache so it can be cleaned up later. `supervisor.run`
      * is atomic and idempotent (it stops any existing entry under the same name). Composite views never passivate, so
      * there is no activation-driven revival: each view is started once from the definition stream and runs
      * continuously.
      */
    private def start(view: ActiveViewDef): IO[Unit] =
      lifecycle.build(view).flatMap { projection =>
        supervisor.run(projection, lifecycle.init(view) >> cache.put(view.ref, view)).void
      }

    private def triggerDestroy(prev: ActiveViewDef, next: CompositeViewDef) = {
      val clear = lifecycle.destroyOnIndexingChange(prev, next) >> cache.remove(prev.ref)
      lifecycle.build(prev).flatMap { compiled =>
        supervisor.destroy(compiled, clear).void
      }
    }

    private def cleanupCurrent(
        next: CompositeViewDef,
        triggerDestroy: (ActiveViewDef, CompositeViewDef) => IO[Unit]
    ): IO[Unit] = {
      val ref = next.ref
      cache.get(ref).flatMap {
        case Some(cached) => triggerDestroy(cached, next)
        case None         => logger.debug(s"View '${ref.project}/${ref.viewId}' is not referenced yet, cleaning is aborted.")
      }
    }

  }

  val metadata: ProjectionMetadata = ProjectionMetadata("system", "composite-views-coordinator", None, None)
  private val logger               = Logger[CompositeViewsCoordinator]

  def apply(
      compositeViews: CompositeViews,
      supervisor: Supervisor,
      builder: CompositeProjectionLifeCycle,
      config: CompositeViewsConfig
  ): IO[CompositeViewsCoordinator] =
    if config.indexingEnabled then apply(compositeViews.views, supervisor, builder)
    else Noop.log.as(Noop)

  def apply(
      fetchViews: Offset => ElemStream[CompositeViewDef],
      supervisor: Supervisor,
      lifecycle: CompositeProjectionLifeCycle
  ): IO[CompositeViewsCoordinator] =
    LocalCache[ViewRef, ActiveViewDef]().flatMap { cache =>
      val coordinator = new Active(fetchViews, cache, supervisor, lifecycle)
      supervisor
        .run(CompiledProjection.fromStream(metadata, ExecutionStrategy.EveryNode, coordinator.run))
        .as(coordinator)
    }

}
