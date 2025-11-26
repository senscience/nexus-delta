package ai.senscience.nexus.delta.sourcing.model

import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import cats.implicits.*
import io.circe.*
import io.circe.Decoder.Result
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

/**
  * Parent type for unique identities as recognized by the system. A client usually has multiple identities with the
  * exception where it performs calls without including an auth token (in which case his only identity is Anonymous).
  */
sealed trait Identity extends Product with Serializable

object Identity {

  /**
    * An identity that has a realm
    */
  sealed trait IdentityRealm extends Identity {

    /**
      * @return
      *   the realm of the identity
      */
    def realm: Label
  }

  /**
    * Parent type for identities that represent a uniquely identified caller.
    */
  sealed trait Subject extends Identity

  /**
    * The Anonymous type.
    */
  type Anonymous = Anonymous.type

  /**
    * The Anonymous singleton identity.
    */
  case object Anonymous extends Subject

  /**
    * A user identity. It represents a unique person or a service account.
    *
    * @param subject
    *   the subject name (usually the preferred_username claim)
    * @param realm
    *   the associated realm that asserts this identity
    */
  final case class User(subject: String, realm: Label) extends Subject with IdentityRealm

  /**
    * A group identity. It asserts that the caller belongs to a certain group of callers.
    *
    * @param group
    *   the group name (asserted by one entry in the groups claim)
    * @param realm
    *   the associated realm that asserts this identity
    */
  final case class Group(group: String, realm: Label) extends IdentityRealm

  /**
    * A role identity. It asserts that the caller belongs to a role.
    *
    * @param role
    *   the role name (asserted by one entry in the roles claim)
    * @param realm
    *   the associated realm that asserts this identity
    */
  final case class Role(role: String, realm: Label) extends IdentityRealm

  /**
    * An authenticated identity is an arbitrary caller that has provided a valid AuthToken issued by a specific realm.
    *
    * @param realm
    *   the realm that asserts this identity
    */
  final case class Authenticated(realm: Label) extends IdentityRealm

  private def decodeAnonymous(hc: HCursor): Result[Subject] =
    hc.get[String]("@type").flatMap {
      case "Anonymous" => Right(Anonymous)
      case _           => Left(DecodingFailure("Cannot decode Anonymous Identity", hc.history))
    }

  private def decodeUser(hc: HCursor): Result[Subject] =
    (hc.get[String]("subject"), hc.get[Label]("realm")).mapN { case (subject, realm) =>
      User(subject, realm)
    }

  private def decodeRole(hc: HCursor): Result[Identity] =
    (hc.get[String]("role"), hc.get[Label]("realm")).mapN { case (role, realm) =>
      Role(role, realm)
    }

  private def decodeGroup(hc: HCursor): Result[Identity] =
    (hc.get[String]("group"), hc.get[Label]("realm")).mapN { case (group, realm) =>
      Group(group, realm)
    }

  private def decodeAuthenticated(hc: HCursor): Result[Identity] =
    hc.get[Label]("realm").map(Authenticated(_))

  private val attempts        =
    List[HCursor => Result[Identity]](decodeAnonymous, decodeUser, decodeRole, decodeGroup, decodeAuthenticated)
  private val attemptsSubject = List[HCursor => Result[Subject]](decodeAnonymous, decodeUser)

  given Decoder[Identity] = {
    Decoder.instance { hc =>
      attempts.foldLeft(Left(DecodingFailure("Unexpected", hc.history)): Result[Identity]) {
        case (acc @ Right(_), _) => acc
        case (_, f)              => f(hc)
      }
    }
  }

  given Decoder[Subject] = Decoder.instance { hc =>
    attemptsSubject.foldLeft(Left(DecodingFailure("Unexpected", hc.history)): Result[Subject]) {
      case (acc @ Right(_), _) => acc
      case (_, f)              => f(hc)
    }
  }

  object Database {
    private given Configuration = Configuration.default.withDiscriminator(keywords.tpe)

    given subjectCodec: Codec.AsObject[Subject]   = deriveConfiguredCodec[Subject]
    given identityCodec: Codec.AsObject[Identity] = deriveConfiguredCodec[Identity]
  }
}
