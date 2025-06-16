package ai.senscience.nexus.delta.provisioning

import ai.senscience.nexus.delta.sdk.ProvisioningAction
import cats.effect.IO
import cats.syntax.all.*

trait ProvisioningCoordinator

object ProvisioningCoordinator extends ProvisioningCoordinator {

  def apply(actions: Vector[ProvisioningAction]): IO[ProvisioningCoordinator] = {
    actions.traverse(_.run).as(ProvisioningCoordinator)
  }

}
