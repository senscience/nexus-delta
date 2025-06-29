package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Label
import pureconfig.ConfigReader

/**
  * Configuration for the application service account.
  *
  * @param value
  *   the service account to be used for internal operations
  */
final case class ServiceAccountConfig(value: ServiceAccount)

object ServiceAccountConfig {
  implicit final val serviceAccountConfigReader: ConfigReader[ServiceAccountConfig] =
    ConfigReader.fromCursor { cursor =>
      for {
        obj      <- cursor.asObjectCursor
        subjectK <- obj.atKey("subject")
        subject  <- ConfigReader[String].from(subjectK)
        realmK   <- obj.atKey("realm")
        realm    <- ConfigReader[Label].from(realmK)
      } yield ServiceAccountConfig(ServiceAccount(User(subject, realm)))
    }
}
