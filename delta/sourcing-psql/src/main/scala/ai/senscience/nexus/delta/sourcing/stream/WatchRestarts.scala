package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.Projections
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectionRestart
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import cats.effect.IO

/**
  * Schedule an internal projection so that restarts can be executed and acknowledged
  */
sealed trait WatchRestarts

object WatchRestarts extends WatchRestarts {

  private[sourcing] val projectionMetadata = ProjectionMetadata("system", "watch-restarts", None, None)

  private val entityType: EntityType = EntityType("projection-restart")

  private def restartId(offset: Offset): Iri = nxv + s"projection/restart/${offset.value}"

  private def success(offset: Offset, restart: ProjectionRestart): SuccessElem[Unit] =
    SuccessElem(
      entityType,
      restartId(offset),
      ProjectRef.unsafe("projection", "restart"),
      restart.instant,
      offset,
      (),
      1
    )

  private def dropped(offset: Offset, restart: ProjectionRestart): Elem.DroppedElem = success(offset, restart).dropped

  // FIXME: Execute watch restarts so that they don't require to be mapped as elems
  def apply(
      supervisor: Supervisor,
      projections: Projections,
      activations: ProjectionActivations
  ): IO[WatchRestarts] =
    supervisor
      .run(
        CompiledProjection.fromStream(
          projectionMetadata,
          ExecutionStrategy.EveryNode,
          (offset: Offset) =>
            projections
              .restarts(offset)
              .evalMap { case (offset, restart) =>
                // Stop the projection and reset its offset on the responsible node, then let the coordinators resume it
                // from the reset offset by broadcasting an activation. Only the responsible node acknowledges.
                supervisor.resetForRestart(restart.metadata.name, restart.fromOffset).flatMap {
                  case true  =>
                    activations.publish(ProjectionActivation.ForProjection(restart.metadata)) >>
                      projections.acknowledgeRestart(offset).as(success(offset, restart))
                  case false =>
                    IO.pure(dropped(offset, restart))
                }
              }
        )
      )
      .as(WatchRestarts)

}
