package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.UnknownAccessTokenIssuer
import ai.senscience.nexus.delta.kernel.jwt.{AuthToken, ParsedToken}
import ai.senscience.nexus.delta.sdk.identities.IdentitiesImpl.logger
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.realms.model.Realm
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.*
import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all.*
import com.nimbusds.jose.jwk.{JWK, JWKSet}
import org.http4s.client.Client
import org.typelevel.otel4s.trace.Tracer

import scala.jdk.CollectionConverters.*
import scala.util.Try

class IdentitiesImpl private[identities] (fetchRealm: FetchRealmByIssuer, fetchRemoteGroups: FetchRemoteGroups)(using
    Tracer[IO]
) extends Identities {

  override def exchange(token: AuthToken): IO[Caller] = {
    def realmKeyset(realm: Realm) = {
      val jwks = realm.keys.foldLeft(Set.empty[JWK]) { case (acc, e) =>
        Try(JWK.parse(e.noSpaces)).map(acc + _).getOrElse(acc)
      }
      new JWKSet(jwks.toList.asJava)
    }

    def fetchGroups(parsedToken: ParsedToken, realm: Realm): IO[Set[Group]] = {
      parsedToken.groups match {
        case Some(groups) => IO.pure(groups)
        case None         => fetchRemoteGroups(realm.userInfoEndpoint, parsedToken)
      }
    }.map(_.map(Group(_, realm.label)))

    val result = for {
      parsedToken <- IO.fromEither(ParsedToken.fromToken(token))
      activeRealm <- OptionT(fetchRealm(parsedToken.issuer)).getOrRaise(UnknownAccessTokenIssuer)
      _           <- IO.fromEither(parsedToken.validate(activeRealm.acceptedAudiences, realmKeyset(activeRealm)))
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
    *   the http client to retrieve groups
    * @param config
    *   the cache configuration
    */
  def apply(realms: Realms, client: Client[IO], config: IdentitiesConfig)(using Tracer[IO]): IO[Identities] = {
    val fetchRealmByIssuer = FetchRealmByIssuer(realms, config.cache)
    val fetchRemoteGroups  = FetchRemoteGroups(config.fetchRemoteGroups, client, config.cache)

    (fetchRealmByIssuer, fetchRemoteGroups).mapN(new IdentitiesImpl(_, _))
  }

}
