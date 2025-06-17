package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.sourcing.EvaluationError.EvaluationTimeout
import ai.senscience.nexus.delta.sourcing.model.EntityType
import ai.senscience.nexus.delta.sourcing.state.State.EphemeralState
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.kernel.error.Rejection

import scala.concurrent.duration.FiniteDuration

final case class EphemeralDefinition[Id, S <: EphemeralState, Command, +R <: Rejection](
    tpe: EntityType,
    evaluate: Command => IO[S],
    stateSerializer: Serializer[Id, S],
    onUniqueViolation: (Id, Command) => R
) {

  /**
    * Fetches the current state and attempt to apply an incoming command on it
    */
  def evaluate(command: Command, maxDuration: FiniteDuration): IO[S] =
    evaluate(command).attempt
      .timeoutTo(maxDuration, IO.raiseError(EvaluationTimeout(command, maxDuration)))
      .rethrow
}
