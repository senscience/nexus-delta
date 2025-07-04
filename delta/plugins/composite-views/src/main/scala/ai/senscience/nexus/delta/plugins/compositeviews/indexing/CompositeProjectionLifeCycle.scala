package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection
import ai.senscience.nexus.delta.plugins.compositeviews.projections.CompositeProjections
import ai.senscience.nexus.delta.plugins.compositeviews.stream.CompositeGraphStream
import ai.senscience.nexus.delta.sourcing.stream.ExecutionStrategy.TransientSingleNode
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, PipeChain}
import cats.effect.IO

/**
  * Handle the different life stages of a composite view projection
  */
trait CompositeProjectionLifeCycle {

  /**
    * Initialise the projection related to the view
    */
  def init(view: ActiveViewDef): IO[Unit]

  /**
    * Build the projection related to the view, applying any matching hook If none, start the regular indexing
    */
  def build(view: ActiveViewDef): IO[CompiledProjection]

  /**
    * Destroy the projection related to the view if changes related to indexing need to be applied
    */
  def destroyOnIndexingChange(prev: ActiveViewDef, next: CompositeViewDef): IO[Unit]
}

object CompositeProjectionLifeCycle {

  private val logger = Logger[CompositeProjectionLifeCycle]

  /**
    * Hook that allows to capture changes to apply before starting the indexing of a composite view
    */
  trait Hook {
    def apply(view: ActiveViewDef): Option[IO[Unit]]
  }

  /**
    * A default implementation that does nothing
    */
  val NoopHook: Hook = (_: ActiveViewDef) => None

  /**
    * Constructs the lifecycle of the projection of a composite view including building the projection itself and how to
    * create/destroy the namespaces and indices related to it
    */
  def apply(
      hooks: Set[Hook],
      compilePipeChain: PipeChain.Compile,
      graphStream: CompositeGraphStream,
      spaces: CompositeSpaces,
      sink: CompositeSinks,
      compositeProjections: CompositeProjections
  ): CompositeProjectionLifeCycle = {
    def init(view: ActiveViewDef): IO[Unit] = spaces.init(view)

    def index(view: ActiveViewDef): IO[CompiledProjection] =
      CompositeViewDef.compile(view, sink, compilePipeChain, graphStream, compositeProjections)

    def destroyAll(view: ActiveViewDef): IO[Unit] =
      for {
        _ <- spaces.destroyAll(view)
        _ <- compositeProjections.deleteAll(view.indexingRef)
      } yield ()

    def destroyProjection(view: ActiveViewDef, projection: CompositeViewProjection): IO[Unit] =
      for {
        _ <- spaces.destroyProjection(view, projection)
        _ <- compositeProjections.partialRebuild(view.ref, projection.id)
      } yield ()

    apply(hooks, init, index, destroyAll, destroyProjection)
  }

  private[indexing] def apply(
      hooks: Set[Hook],
      onInit: ActiveViewDef => IO[Unit],
      index: ActiveViewDef => IO[CompiledProjection],
      destroyAll: ActiveViewDef => IO[Unit],
      destroyProjection: (ActiveViewDef, CompositeViewProjection) => IO[Unit]
  ): CompositeProjectionLifeCycle = new CompositeProjectionLifeCycle {

    override def init(view: ActiveViewDef): IO[Unit] = onInit(view)

    override def build(view: ActiveViewDef): IO[CompiledProjection] = {
      detectHook(view).getOrElse {
        index(view)
      }
    }

    private def detectHook(view: ActiveViewDef) = {
      val initial: Option[IO[Unit]] = None
      hooks.toList
        .foldLeft(initial) { case (acc, hook) =>
          (acc ++ hook(view)).reduceOption(_ >> _)
        }
        .map { task =>
          IO.pure(CompiledProjection.fromTask(view.metadata, TransientSingleNode, task))
        }
    }

    override def destroyOnIndexingChange(prev: ActiveViewDef, next: CompositeViewDef): IO[Unit] =
      (prev, next) match {
        case (prev, next) if prev.ref != next.ref                                            =>
          IO.raiseError(new IllegalArgumentException(s"Different views were provided: '${prev.ref}' and '${next.ref}'"))
        case (prev, _: DeprecatedViewDef)                                                    =>
          logger.info(s"View '${prev.ref}' has been deprecated, cleaning up the current one.") >> destroyAll(prev)
        case (prev, nextActive: ActiveViewDef) if prev.indexingRev != nextActive.indexingRev =>
          logger.info(s"View '${prev.ref}' sources have changed, cleaning up the current one..") >> destroyAll(prev)
        case (prev, nextActive: ActiveViewDef)                                               =>
          checkProjections(prev, nextActive)
      }

    private def checkProjections(prev: ActiveViewDef, nextActive: ActiveViewDef) =
      prev.projections.nonEmptyTraverse { prevProjection =>
        nextActive.projection(prevProjection.id) match {
          case Right(nextProjection) =>
            IO.whenA(prevProjection.indexingRev != nextProjection.indexingRev)(
              logger.info(
                s"Projection ${prevProjection.id} of view '${prev.ref}' has changed, cleaning up the current one.."
              ) >>
                destroyProjection(prev, prevProjection)
            )
          case Left(_)               =>
            logger.info(
              s"Projection ${prevProjection.id} of view '${prev.ref}' was removed, cleaning up the current one.."
            ) >>
              destroyProjection(prev, prevProjection)
        }
      }.void
  }

}
