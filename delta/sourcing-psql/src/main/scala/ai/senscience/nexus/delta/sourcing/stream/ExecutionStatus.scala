package ai.senscience.nexus.delta.sourcing.stream

import io.circe.{Encoder, Json}

/**
  * Enumeration of projection execution statuses.
  */
sealed trait ExecutionStatus extends Product with Serializable {

  /**
    * @param th
    *   the error to set on the failed status
    * @return
    *   a failed status with the same offset as this and provided error
    */
  def failed(th: Throwable): ExecutionStatus.Failed = ExecutionStatus.Failed(th)

  /**
    * @return
    *   true if the status is [[ExecutionStatus.Stopped]], false otherwise
    */
  def isStopped: Boolean = false

  /**
    * @return
    *   true if the status is [[ExecutionStatus.Running]], false otherwise
    */
  def isRunning: Boolean = false
}

object ExecutionStatus {

  /**
    * Status for projections that are prepared for executions.
    */
  case object Pending extends ExecutionStatus

  /**
    * Status for projections that are running.
    */
  case object Running extends ExecutionStatus {
    override def isRunning: Boolean = true
  }

  /**
    * Status for projections that have stopped.
    */
  case object Stopped extends ExecutionStatus {
    override def isStopped: Boolean = true
  }

  /**
    * Status for projections that have completed.
    */
  case object Completed extends ExecutionStatus

  /**
    * Status for projections that have failed.
    * @param th
    *   the error that failed the projection
    */
  final case class Failed(th: Throwable) extends ExecutionStatus

  given Encoder[ExecutionStatus] =
    Encoder.instance[ExecutionStatus] {
      case Pending   => Json.fromString("Pending")
      case Running   => Json.fromString("Running")
      case Stopped   => Json.fromString("Stopped")
      case Completed => Json.fromString("Completed")
      case Failed(_) => Json.fromString("Failed")
    }
}
