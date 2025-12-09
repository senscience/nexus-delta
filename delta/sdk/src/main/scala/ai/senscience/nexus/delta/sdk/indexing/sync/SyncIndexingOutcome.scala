package ai.senscience.nexus.delta.sdk.indexing.sync

import cats.Semigroup

sealed trait SyncIndexingOutcome

object SyncIndexingOutcome {

  case object Success extends SyncIndexingOutcome

  final case class Failed(errors: List[Throwable]) extends SyncIndexingOutcome

  given Semigroup[SyncIndexingOutcome] = (x: SyncIndexingOutcome, y: SyncIndexingOutcome) =>
    (x, y) match {
      case (Success, Success)               => Success
      case (Success, error: Failed)         => error
      case (error: Failed, Success)         => error
      case (error1: Failed, error2: Failed) => Failed(error1.errors ++ error2.errors)
    }

}
