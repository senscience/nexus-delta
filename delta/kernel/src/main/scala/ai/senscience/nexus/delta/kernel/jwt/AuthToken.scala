package ai.senscience.nexus.delta.kernel.jwt

/**
  * Data type representing a authentication token, usually a OAuth2 bearer token.
  *
  * @param value
  *   the string representation of the token
  */
final case class AuthToken(value: String) extends AnyVal
