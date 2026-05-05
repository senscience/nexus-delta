package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.cache.CacheConfig
import ai.senscience.nexus.delta.sdk.generators.{RealmGen, WellKnownGen}
import ai.senscience.nexus.delta.sdk.realms.WellKnownResolver
import ai.senscience.nexus.delta.sdk.realms.model.RealmRejection.UnsuccessfulOpenIdConfigResponse
import ai.senscience.nexus.delta.sdk.realms.model.WellKnown
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import cats.effect.kernel.Ref
import io.circe.Json
import org.http4s.Uri
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

class FetchJwksSuite extends NexusSuite with CirceLiteral {

  private val cacheConfig = CacheConfig(enabled = true, 10, 5.minutes)

  private val (githubOpenId, githubWk) = WellKnownGen.create("github")
  private val githubRealm              = RealmGen.realm(githubOpenId, githubWk)

  private val rotatedKey = json"""{ "k": "rotated" }"""

  private def stubResolver(callsRef: Ref[IO, Int], wkSeq: List[WellKnown]): WellKnownResolver =
    (configUri: Uri) =>
      callsRef
        .modify { i =>
          val next = wkSeq.lift(i).getOrElse(wkSeq.last)
          (i + 1, next)
        }
        .flatMap {
          case wk if configUri == githubOpenId => IO.pure(wk)
          case _                               => IO.raiseError(UnsuccessfulOpenIdConfigResponse(configUri))
        }

  test("Fetch the keys once and serve subsequent calls from the cache") {
    for {
      calls     <- Ref[IO].of(0)
      fetchJwks <- FetchJwks(stubResolver(calls, List(githubWk)), cacheConfig)
      _         <- fetchJwks(githubRealm)
      _         <- fetchJwks(githubRealm)
      _         <- fetchJwks(githubRealm)
      _         <- calls.get.assertEquals(1)
    } yield ()
  }

  test("Refresh forces a new fetch and overwrites the cached entry") {
    val rotatedWk = githubWk.copy(keys = Set(rotatedKey))
    for {
      calls     <- Ref[IO].of(0)
      fetchJwks <- FetchJwks(stubResolver(calls, List(githubWk, rotatedWk)), cacheConfig)
      first     <- fetchJwks(githubRealm)
      _          = assertEquals(first.getKeys.size, 0) // initial stubbed key isn't a valid JWK
      _         <- fetchJwks.refresh(githubRealm)
      // Subsequent apply call should now return the refreshed value (still cached, no new fetch)
      _         <- fetchJwks(githubRealm)
      _         <- calls.get.assertEquals(2)
    } yield ()
  }

  test("Surface errors raised by the underlying WellKnownResolver") {
    val failing: WellKnownResolver = (uri: Uri) => IO.raiseError(UnsuccessfulOpenIdConfigResponse(uri))
    for {
      fetchJwks <- FetchJwks(failing, cacheConfig)
      _         <- fetchJwks(githubRealm).interceptEquals(UnsuccessfulOpenIdConfigResponse(githubOpenId))
    } yield ()
  }

  test("Build a JWKSet that contains all valid RSA keys returned by the resolver") {
    import com.nimbusds.jose.jwk.gen.RSAKeyGenerator
    val rsa1                        = new RSAKeyGenerator(2048).keyID("k1").generate().toPublicJWK
    val rsa2                        = new RSAKeyGenerator(2048).keyID("k2").generate().toPublicJWK
    val keysJson: Set[Json]         = Set(rsa1, rsa2).map(k => io.circe.parser.parse(k.toJSONString).toOption.get)
    val wkWithRsa: WellKnown        = githubWk.copy(keys = keysJson)
    val resolver: WellKnownResolver = (_: Uri) => IO.pure(wkWithRsa)
    for {
      fetchJwks <- FetchJwks(resolver, cacheConfig)
      jwks      <- fetchJwks(githubRealm)
      _          = assertEquals(jwks.getKeys.size, 2)
    } yield ()
  }
}
