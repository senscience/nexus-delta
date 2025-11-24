package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.cache.{CacheConfig, LocalCache}
import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.RealmSearchParams
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.realms.model.Realm
import cats.effect.IO

/**
  * Fetch the active realm matching the issuer
  */
trait FetchRealmByIssuer {
  def apply(issuer: String): IO[Option[Realm]]
}

object FetchRealmByIssuer {

  def apply(realms: Realms, config: CacheConfig): IO[FetchRealmByIssuer] =
    LocalCache[String, Realm](config).map { cache =>
      new FetchRealmByIssuer {
        private val pagination = FromPagination(0, 1000)
        private val sort       = ResourceF.defaultSort[Realm]

        override def apply(issuer: String): IO[Option[Realm]] =
          cache.getOrElseAttemptUpdate(issuer, internalFetch(issuer))

        private def internalFetch(issuer: String) = {
          val params = RealmSearchParams(issuer = Some(issuer), deprecated = Some(false))
          realms.list(pagination, params, sort).map {
            _.results.map(_.source.value).headOption
          }
        }
      }
    }
}
