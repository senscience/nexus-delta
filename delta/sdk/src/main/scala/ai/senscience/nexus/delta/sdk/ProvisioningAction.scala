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

  enum Outcome {
    case Success

    case Skipped

    case Disabled

    case Error
  }
}
