package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.{InvalidAccessToken, UnknownAccessTokenIssuer}
import ai.senscience.nexus.delta.kernel.jwt.{AuthToken, ParsedToken}
import ai.senscience.nexus.delta.sdk.identities.IdentitiesImpl.logger
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.*
import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all.*
import org.http4s.client.Client
import org.typelevel.otel4s.trace.Tracer

class IdentitiesImpl private[identities] (
    realmResolver: RealmResolver,
    fetchRemoteGroups: FetchRemoteGroups
)(using Tracer[IO])
    extends Identities {

  override def exchange(token: AuthToken): IO[Caller] = {
    def fetchGroups(parsedToken: ParsedToken, realm: ResolvedRealm): IO[Set[Group]] = {
      parsedToken.groups match {
        case Some(groups) => IO.pure(groups)
        case None         => fetchRemoteGroups(realm.userInfoEndpoint, parsedToken)
      }
    }.map(_.map(Group(_, realm.label)))

    def validate(parsedToken: ParsedToken, realm: ResolvedRealm): IO[Unit] =
      IO.fromEither(parsedToken.validate(realm.acceptedAudiences, realm.jwks))

    def validateWithRetry(parsedToken: ParsedToken, realm: ResolvedRealm): IO[Unit] =
      validate(parsedToken, realm).recoverWith { case _: InvalidAccessToken =>
        realmResolver.refresh(realm).flatMap(validate(parsedToken, _))
      }

    val result = for {
      parsedToken <- IO.fromEither(ParsedToken.fromToken(token))
      activeRealm <- OptionT(realmResolver(parsedToken.issuer)).getOrRaise(UnknownAccessTokenIssuer)
      _           <- validateWithRetry(parsedToken, activeRealm)
      groups      <- fetchGroups(parsedToken, activeRealm)
      roles        = parsedToken.roles.fold(Set.empty)(_.map { r => Role(r, activeRealm.label) })
    } yield {
      val user = User(parsedToken.subject, activeRealm.label)
      Caller(user, roles ++ groups ++ Set(Anonymous, user, Authenticated(activeRealm.label)))
    }
    result.surround("exchangeToken")
  }.onError { case rejection =>
    logger.debug(s"Extracting and validating the caller failed for the reason: $rejection")
  }
}

object IdentitiesImpl {

  private val logger = Logger[this.type]

  /**
    * Constructs a [[IdentitiesImpl]] instance
    *
    * @param realms
    *   the realms instance
    * @param client
    *   the http client to retrieve groups and JWKS keys
    * @param config
    *   the cache configuration
    */
  def apply(realms: Realms, client: Client[IO], config: IdentitiesConfig)(using Tracer[IO]): IO[Identities] = {
    val realmResolver     = RealmResolver(realms, client, config.cache)
    val fetchRemoteGroups = FetchRemoteGroups(config.fetchRemoteGroups, client, config.cache)

    (realmResolver, fetchRemoteGroups).mapN(new IdentitiesImpl(_, _))
  }

}
