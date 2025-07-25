package ai.senscience.nexus.delta.sourcing

import scala.concurrent.duration.FiniteDuration

/**
  * Error that may occur when evaluating commands or replaying a state
  */
sealed abstract class EvaluationError(message: String) extends Exception(message) with Product with Serializable {
  self =>
  override def fillInStackTrace(): Throwable = self
}

object EvaluationError {

  /**
    * Error occurring when applying an event to a state result in an invalid one
    * @param state
    *   the original state
    * @param event
    *   the event to apply
    */
  final case class InvalidState[State, Event](state: Option[State], event: Event)
      extends EvaluationError(s"Applying the event $event on the original state $state resulted in an invalid state")

  /**
    * Error occurring when the evaluation of a command exceeds the defined timeout
    * @param command
    *   the command that failed
    * @param timeoutAfter
    *   the timeout that was applied
    */
  final case class EvaluationTimeout[Command](command: Command, timeoutAfter: FiniteDuration)
      extends EvaluationError(s"'$command' received a timeout after $timeoutAfter")

  /**
    * Error when the tagged state can't be correctly computed during a tag operation
    *
    * @param command
    *   the command that failed
    * @param lastRev
    *   the found revision for the computed state
    */
  final case class EvaluationTagFailure[Command](command: Command, lastRev: Option[Int])
      extends EvaluationError(
        s"'$command' could not compute the tagged state, the state could only be found until rev '$lastRev'"
      )

}
