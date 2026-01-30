package ai.senscience.nexus.delta.sdk.realms

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.ProvisioningAction
import ai.senscience.nexus.delta.sdk.ProvisioningAction.Outcome
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.realms.RealmProvisioning.logger
import ai.senscience.nexus.delta.sdk.realms.model.RealmRejection.RealmAlreadyExists
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import cats.effect.IO
import cats.syntax.all.*

/**
  * Provision the different realms provided in the configuration
  */
final class RealmProvisioning(realms: Realms, config: RealmsProvisioningConfig, serviceAccount: ServiceAccount)
    extends ProvisioningAction {

  override def run: IO[ProvisioningAction.Outcome] =
    if config.enabled then {
      given Subject = serviceAccount.subject
      for {
        _ <- logger.info(s"Realm provisioning is active. Creating ${config.realms.size} realms...")
        _ <- config.realms.toList.traverse { case (label, fields) =>
               realms.create(label, fields).recoverWith {
                 case r: RealmAlreadyExists => logger.debug(r)(s"Realm '$label' already exists")
                 case e                     => logger.error(e)(s"Realm '$label' could not be created: '${e.getMessage}'")
               }
             }
        _ <- logger.info(s"Provisioning ${config.realms.size} realms is completed")
      } yield Outcome.Success
    } else logger.info("Realm provisioning is inactive.").as(Outcome.Disabled)
}

object RealmProvisioning {

  private val logger = Logger[RealmProvisioning]

}
