package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.jwt.AuthToken
import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.InvalidAccessToken
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Authenticated, Group, User}
import cats.effect.IO

/**
  * Dummy implementation of [[Identities]] passing the expected results in a map
  */
class IdentitiesDummy private (expected: Map[AuthToken, Caller]) extends Identities {

  override def exchange(token: AuthToken): IO[Caller] =
    IO.fromEither(
      expected.get(token).toRight(InvalidAccessToken("Someone", "Some realm", "The caller could not be found."))
    )
}

object IdentitiesDummy {

  /**
    * Create a new dummy Identities implementation from a list of callers
    */
  def apply(expected: Caller*): Identities =
    new IdentitiesDummy(expected.flatMap { c =>
      c.subject match {
        case User(subject, _) => Some(AuthToken(subject) -> c)
        case _                => None
      }
    }.toMap)

  /**
    * Create a new dummy Identities implementation from a list of users
    */
  def fromUsers(users: User*): Identities =
    new IdentitiesDummy(users.map { u =>
      val caller = Caller(u, Set(u, Anonymous, Authenticated(u.realm), Group("group", u.realm)))
      AuthToken(u.subject) -> caller
    }.toMap)
}
