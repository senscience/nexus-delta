package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.error.ServiceError.IndexingFailed
import ai.senscience.nexus.delta.sdk.indexing.IndexingMode.{Async, Sync}
import ai.senscience.nexus.delta.sdk.indexing.sync.SyncIndexingOutcome
import ai.senscience.nexus.delta.sdk.indexing.sync.SyncIndexingOutcome.{Failed, Success}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem
import ai.senscience.nexus.delta.sourcing.stream.Elem.{FailedElem, SuccessElem}
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*

trait SyncIndexingAction {
  def apply[A](entityType: EntityType)(project: ProjectRef, res: ResourceF[A]): IO[SyncIndexingOutcome]
}

object SyncIndexingAction {

  type Execute[A] = (ProjectRef, ResourceF[A], IndexingMode) => IO[Unit]

  def evalMapToElem[A, B](
      entityType: EntityType
  )(project: ProjectRef, res: ResourceF[A], f: ResourceF[A] => IO[B]): IO[Elem[B]] =
    f(res).redeem(
      err =>
        FailedElem(
          entityType,
          res.id,
          project,
          res.updatedAt,
          Offset.Start,
          err,
          res.rev
        ),
      value => SuccessElem(entityType, res.id, project, res.updatedAt, Offset.Start, value, res.rev)
    )

  /**
    * Does not perform any action
    */
  def noop[A]: Execute[A] = (_, _, _) => IO.unit

  private val logger = Logger[SyncIndexingAction]

  /**
    * An instance of [[SyncIndexingAction]] which executes other [[SyncIndexingAction]] s in parallel.
    */
  final class AggregateIndexingAction(private val internal: NonEmptyList[SyncIndexingAction])
      extends SyncIndexingAction {

    def apply[A](entityType: EntityType)(project: ProjectRef, res: ResourceF[A], indexingMode: IndexingMode): IO[Unit] =
      indexingMode match {
        case Async => IO.unit
        case Sync  =>
          logger.debug(s"Synchronous indexing of resource '$project/${res.id}' has been requested.") >>
            apply(entityType)(project, res).flatMap {
              case Success        => IO.unit
              case Failed(errors) => IO.raiseError(IndexingFailed(res.void, errors))
            }
      }

    override def apply[A](entityType: EntityType)(project: ProjectRef, res: ResourceF[A]): IO[SyncIndexingOutcome] =
      internal.parTraverse(_.apply(entityType)(project, res)).map(_.reduce)
  }

  object AggregateIndexingAction {
    def apply(internal: NonEmptyList[SyncIndexingAction]): AggregateIndexingAction =
      new AggregateIndexingAction(internal)
  }
}
