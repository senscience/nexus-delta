package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.utils.FileUtils
import ai.senscience.nexus.delta.sdk.ProvisioningAction
import ai.senscience.nexus.delta.sdk.ProvisioningAction.Outcome
import ai.senscience.nexus.delta.sdk.acls.AclProvisioning.logger
import ai.senscience.nexus.delta.sdk.acls.model.AclBatchReplace
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import cats.effect.IO
import cats.syntax.all.*

/**
  * Provision the different acls provided in the file defined by the configuration
  */
final class AclProvisioning(acls: Acls, config: AclProvisioningConfig, serviceAccount: ServiceAccount)
    extends ProvisioningAction {

  override def run: IO[ProvisioningAction.Outcome] =
    if config.enabled then {
      acls.isRootAclSet.flatMap {
        case true  =>
          logger.warn("Root acls are already set in this instance, skipping the provisioning...").as(Outcome.Skipped)
        case false =>
          config.path
            .traverse(FileUtils.loadJsonAs[AclBatchReplace])
            .flatMap {
              case Some(input) => loadAcls(input, acls, serviceAccount).as(Outcome.Success)
              case None        => logger.error("No acl provisioning file has been defined.").as(Outcome.Error)
            }
            .recoverWith { e =>
              logger.error(e)(s"Acl provisionning failed because of '${e.getMessage}'.").as(Outcome.Error)
            }
      }
    } else logger.info("Acl provisioning is inactive.").as(Outcome.Disabled)

  private def loadAcls(input: AclBatchReplace, acls: Acls, serviceAccount: ServiceAccount) = {
    logger.info(s"Provisioning ${input.acls.size} acl entries...") >>
      input.acls.traverse { acl =>
        acls.replace(acl, 0)(using serviceAccount.subject).recoverWith { e =>
          logger.error(e)(s"Acl for address '${acl.address}' could not be set: '${e.getMessage}.")
        }
      }.void >> logger.info("Provisioning acls is completed.")
  }
}

object AclProvisioning {

  private val logger = Logger[AclProvisioning]
}
