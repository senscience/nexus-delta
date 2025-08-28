package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.ResourceShift
import ai.senscience.nexus.delta.sdk.error.ServiceError.IndexingFailed
import ai.senscience.nexus.delta.sdk.indexing.IndexingAction.IndexingActionContext
import ai.senscience.nexus.delta.sdk.indexing.IndexingMode.{Async, Sync}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, Elem, Projection}
import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream

import scala.concurrent.duration.*

trait IndexingAction {

  implicit private val bc: BatchConfig = BatchConfig.individual

  protected def kamonMetricComponent: KamonMetricComponent

  /**
    * The maximum duration accepted to perform the synchronous indexing
    * @return
    */
  def timeout: FiniteDuration

  /**
    * Initialize the indexing projections to perform for the given element
    */
  def projections(project: ProjectRef, elem: Elem[GraphResource]): Stream[IO, CompiledProjection]

  def apply(project: ProjectRef, elem: Elem[GraphResource], context: IndexingActionContext): IO[Unit] =
    projections(project, elem)
      .evalMap(runProjection(_, context))
      .compile
      .drain
      .span("sync-indexing")(kamonMetricComponent)

  private def runProjection(compiled: CompiledProjection, context: IndexingActionContext) = {
    for {
      projection <- Projection(compiled, IO.none, _ => IO.unit, context.addErrors)
      _          <- projection.waitForCompletion(timeout)
      // We stop the projection if it has not complete yet
      _          <- projection.stop()
    } yield ()
  }.handleErrorWith { context.addError }
}

object IndexingAction {

  type Execute[A] = (ProjectRef, ResourceF[A], IndexingMode) => IO[Unit]

  private type ErrorList = List[Throwable]

  /**
    * Does not perform any action
    */
  def noop[A]: Execute[A] = (_, _, _) => IO.unit

  private val logger = Logger[IndexingAction]

  final class IndexingActionContext(value: Ref[IO, ErrorList]) {
    def addError(error: Throwable): IO[Unit] = value.update(_.+:(error))

    def addErrors(failed: List[FailedElem]): IO[Unit] = value.update(_ ++ failed.map(_.throwable))

    def get: IO[ErrorList] = value.get
  }

  object IndexingActionContext {
    def apply(): IO[IndexingActionContext] =
      Ref.of[IO, ErrorList](List.empty).map(new IndexingActionContext(_))
  }

  /**
    * An instance of [[IndexingAction]] which executes other [[IndexingAction]] s in parallel.
    */
  final class AggregateIndexingAction(private val internal: NonEmptyList[IndexingAction])(implicit
      cr: RemoteContextResolution
  ) {

    def apply[A](project: ProjectRef, res: ResourceF[A], indexingMode: IndexingMode)(implicit
        shift: ResourceShift[?, A]
    ): IO[Unit] =
      indexingMode match {
        case Async => IO.unit
        case Sync  =>
          for {
            _            <- logger.debug(s"Synchronous indexing of resource '$project/${res.id}' has been requested.")
            // We create the GraphResource wrapped in an `Elem`
            resourceElem <- shift.toGraphResourceElem(project, res)
            context      <- IndexingActionContext()
            _            <- internal.parTraverse(_.apply(project, resourceElem, context))
            errors       <- context.get
            _            <- IO.raiseWhen(errors.nonEmpty)(IndexingFailed(res.void, errors))
          } yield ()
      }
  }

  object AggregateIndexingAction {
    def apply(
        internal: NonEmptyList[IndexingAction]
    )(implicit cr: RemoteContextResolution): AggregateIndexingAction =
      new AggregateIndexingAction(internal)
  }
}
