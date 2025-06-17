package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.jwt.AuthToken
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import cats.effect.IO

/**
  * Operations pertaining to authentication, token validation and identities.
  */
trait Identities {

  /**
    * Attempt to exchange a token for a specific validated Caller.
    *
    * @param token
    *   a well formatted authentication token (usually a bearer token)
    */
  def exchange(token: AuthToken): IO[Caller]

}
