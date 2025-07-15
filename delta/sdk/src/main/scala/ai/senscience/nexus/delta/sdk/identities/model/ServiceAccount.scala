package ai.senscience.nexus.delta.sdk.identities.model

import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import pureconfig.ConfigReader

/**
  * A service account backed by a subject.
  *
  * @param subject
  *   the underlying service account subject
  */
final case class ServiceAccount(subject: Subject) extends AnyVal {

  /**
    * @return
    *   the collection of identities of this service account
    */
  def identities: Set[Identity] = Set(subject)

  /**
    * @return
    *   a [[Caller]] representation for this service account
    */
  def caller: Caller = Caller(subject, identities)
}

object ServiceAccount {
  implicit final val serviceAccountReader: ConfigReader[ServiceAccount] =
    ConfigReader.fromCursor { cursor =>
      for {
        obj      <- cursor.asObjectCursor
        subjectK <- obj.atKey("subject")
        subject  <- ConfigReader[String].from(subjectK)
        realmK   <- obj.atKey("realm")
        realm    <- ConfigReader[Label].from(realmK)
      } yield ServiceAccount(User(subject, realm))
    }
}
