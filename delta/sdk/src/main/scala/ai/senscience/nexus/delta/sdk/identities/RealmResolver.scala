package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.cache.{CacheConfig, LocalCache}
import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.RealmSearchParams
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.realms.WellKnownResolver
import ai.senscience.nexus.delta.sdk.realms.model.Realm
import ai.senscience.nexus.delta.sourcing.model.Label
import cats.data.NonEmptySet
import cats.effect.IO
import com.nimbusds.jose.jwk.{JWK, JWKSet}
import io.circe.Json
import org.http4s.Uri
import org.http4s.client.Client

import scala.jdk.CollectionConverters.*
import scala.util.Try

/**
  * The active realm matching a token issuer, reduced to the fields required to validate a token and build the caller.
  */
final case class ResolvedRealm(
    issuer: String,
    label: Label,
    acceptedAudiences: Option[NonEmptySet[String]],
    openIdConfig: Uri,
    userInfoEndpoint: Uri,
    jwks: JWKSet
)

object ResolvedRealm {
  def apply(realm: Realm, jwks: JWKSet): ResolvedRealm =
    ResolvedRealm(realm.issuer, realm.label, realm.acceptedAudiences, realm.openIdConfig, realm.userInfoEndpoint, jwks)
}

/**
  * Resolves the active realm and its JWKS for a given token issuer, caching the result in a single local cache.
  */
trait RealmResolver {

  /**
    * Resolve the active realm matching the given issuer, fetching its JWKS in the process.
    */
  def apply(issuer: String): IO[Option[ResolvedRealm]]

  /**
    * Force a refresh of the JWKS for an already resolved realm, overwriting the cached entry.
    */
  def refresh(resolved: ResolvedRealm): IO[ResolvedRealm]

}

object RealmResolver {

  final private class Impl(
      cache: LocalCache[String, ResolvedRealm],
      realms: Realms,
      wellKnown: WellKnownResolver
  ) extends RealmResolver {

    private val pagination = FromPagination(0, 1000)
    private val sort       = ResourceF.defaultSort[Realm]

    override def apply(issuer: String): IO[Option[ResolvedRealm]] =
      cache.getOrElseAttemptUpdate(issuer, resolve(issuer))

    override def refresh(resolved: ResolvedRealm): IO[ResolvedRealm] =
      fetchJwks(resolved.openIdConfig)
        .map(jwks => resolved.copy(jwks = jwks))
        .flatTap(updated => cache.put(updated.issuer, updated))

    private def resolve(issuer: String): IO[Option[ResolvedRealm]] =
      fetchRealm(issuer).flatMap {
        case Some(realm) => fetchJwks(realm.openIdConfig).map(jwks => Some(ResolvedRealm(realm, jwks)))
        case None        => IO.none
      }

    private def fetchRealm(issuer: String): IO[Option[Realm]] = {
      val params = RealmSearchParams(issuer = Some(issuer), deprecated = Some(false))
      realms.list(pagination, params, sort).map {
        _.results.map(_.source.value).headOption
      }
    }

    private def fetchJwks(openIdConfig: Uri): IO[JWKSet] =
      wellKnown(openIdConfig).map(wk => toJwkSet(wk.keys))
  }

  private def toJwkSet(keys: Set[Json]): JWKSet = {
    val parsed = keys.foldLeft(Set.empty[JWK]) { case (acc, e) =>
      Try(JWK.parse(e.noSpaces)).map(acc + _).getOrElse(acc)
    }
    new JWKSet(parsed.toList.asJava)
  }

  def apply(realms: Realms, client: Client[IO], config: CacheConfig): IO[RealmResolver] =
    apply(realms, WellKnownResolver(client), config)

  def apply(realms: Realms, wellKnown: WellKnownResolver, config: CacheConfig): IO[RealmResolver] =
    LocalCache[String, ResolvedRealm](config).map(new Impl(_, realms, wellKnown))

}
