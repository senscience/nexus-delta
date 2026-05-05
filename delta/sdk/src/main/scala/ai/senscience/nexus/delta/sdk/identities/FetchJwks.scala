package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.cache.{CacheConfig, LocalCache}
import ai.senscience.nexus.delta.sdk.realms.WellKnownResolver
import ai.senscience.nexus.delta.sdk.realms.model.Realm
import ai.senscience.nexus.delta.sourcing.model.Label
import cats.effect.IO
import com.nimbusds.jose.jwk.{JWK, JWKSet}
import io.circe.Json
import org.http4s.client.Client

import scala.jdk.CollectionConverters.*
import scala.util.Try

trait FetchJwks {

  def apply(realm: Realm): IO[JWKSet]

  def refresh(realm: Realm): IO[JWKSet]

}

object FetchJwks {

  final private class Active(cache: LocalCache[Label, JWKSet], wellKnown: WellKnownResolver) extends FetchJwks {

    override def apply(realm: Realm): IO[JWKSet] =
      cache.getOrElseUpdate(realm.label, internalFetch(realm))

    override def refresh(realm: Realm): IO[JWKSet] =
      internalFetch(realm).flatTap(cache.put(realm.label, _))

    private def internalFetch(realm: Realm): IO[JWKSet] =
      wellKnown(realm.openIdConfig).map(wk => toJwkSet(wk.keys))
  }

  private def toJwkSet(keys: Set[Json]): JWKSet = {
    val parsed = keys.foldLeft(Set.empty[JWK]) { case (acc, e) =>
      Try(JWK.parse(e.noSpaces)).map(acc + _).getOrElse(acc)
    }
    new JWKSet(parsed.toList.asJava)
  }

  def apply(client: Client[IO], cacheConfig: CacheConfig): IO[FetchJwks] =
    apply(WellKnownResolver(client), cacheConfig)

  def apply(wellKnown: WellKnownResolver, cacheConfig: CacheConfig): IO[FetchJwks] =
    LocalCache[Label, JWKSet](cacheConfig).map(new Active(_, wellKnown))

}
