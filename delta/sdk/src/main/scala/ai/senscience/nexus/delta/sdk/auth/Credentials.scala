package ai.senscience.nexus.delta.sdk.auth

import ai.senscience.nexus.delta.kernel.Secret
import ai.senscience.nexus.delta.sourcing.model.Label
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Enumerates the different ways to obtain an auth toke for making requests to a remote service
  */
sealed trait Credentials

object Credentials {

  /**
    * When no auth token should be used
    */
  case object Anonymous extends Credentials {
    given ConfigReader[Anonymous.type] = deriveReader[Anonymous.type]
  }

  /**
    * When a long-lived auth token should be used (legacy, not recommended)
    */
  case class JWTToken(token: String) extends Credentials
  case object JWTToken {
    given ConfigReader[JWTToken] = deriveReader[JWTToken]
  }

  /**
    * When client credentials should be exchanged with an OpenId service to obtain an auth token
    * @param realm
    *   the realm which defines the OpenId service
    */
  case class ClientCredentials(user: String, password: Secret[String], realm: Label) extends Credentials
  object ClientCredentials {
    given ConfigReader[ClientCredentials] = deriveReader[ClientCredentials]
  }

  given ConfigReader[Credentials] = deriveReader[Credentials]
}
