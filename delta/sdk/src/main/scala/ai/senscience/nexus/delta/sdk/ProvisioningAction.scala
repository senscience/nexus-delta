package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.sdk.ProvisioningAction.Outcome
import cats.effect.IO

/**
  * Provisioning action to run at startup
  */
trait ProvisioningAction {

  def run: IO[Outcome]

}

object ProvisioningAction {

  sealed trait Outcome

  object Outcome {
    case object Success  extends Outcome
    case object Skipped  extends Outcome
    case object Disabled extends Outcome
    case object Error    extends Outcome
  }

}
