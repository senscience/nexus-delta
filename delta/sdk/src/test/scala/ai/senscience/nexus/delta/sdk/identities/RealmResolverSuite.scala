package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.generators.WellKnownGen
import ai.senscience.nexus.delta.sdk.model.Name
import ai.senscience.nexus.delta.sdk.realms.model.RealmFields
import ai.senscience.nexus.delta.sdk.realms.model.RealmRejection.UnsuccessfulOpenIdConfigResponse
import ai.senscience.nexus.delta.sdk.realms.model.WellKnown
import ai.senscience.nexus.delta.sdk.realms.{RealmsConfig, RealmsImpl, RealmsProvisioningConfig, WellKnownResolver}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref
import io.circe.Json
import munit.AnyFixture
import org.http4s.Uri

class RealmResolverSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures with CirceLiteral {

  private lazy val xas = doobie()

  private val (github, gitlab)         = (Label.unsafe("github"), Label.unsafe("gitlab"))
  private val githubName               = Name.unsafe("github-name")
  private val gitlabName               = Name.unsafe("gitlab-name")
  private val (githubOpenId, githubWk) = WellKnownGen.create(github.value)
  private val (gitlabOpenId, gitlabWk) = WellKnownGen.create(gitlab.value)

  private val rotatedKey = json"""{ "k": "rotated" }"""

  private val wkResolver: WellKnownResolver = {
    case `githubOpenId` => IO.pure(githubWk)
    case `gitlabOpenId` => IO.pure(gitlabWk)
    case other          => IO.raiseError(UnsuccessfulOpenIdConfigResponse(other))
  }

  // A resolver counting the number of well-known fetches, serving the given sequence of well-knowns
  private def countingResolver(callsRef: Ref[IO, Int], wkSeq: List[WellKnown]): WellKnownResolver =
    (configUri: Uri) =>
      callsRef
        .modify(i => (i + 1, wkSeq.lift(i).getOrElse(wkSeq.last)))
        .flatMap {
          case wk if configUri == githubOpenId => IO.pure(wk)
          case _                               => IO.raiseError(UnsuccessfulOpenIdConfigResponse(configUri))
        }

  private val provisioning = RealmsProvisioningConfig(enabled = false, Map.empty)
  private val config       = RealmsConfig(eventLogConfig, pagination, provisioning)
  private lazy val realms  = RealmsImpl(config, wkResolver, xas, clock)

  // Seed the realms once for the whole suite: an active github realm and a deprecated gitlab realm
  private val seedRealms = ResourceSuiteLocalFixture(
    "seedRealms",
    Resource.eval {
      IO.defer {
        given Subject = Identity.User("user", Label.unsafe("realm"))
        for {
          _ <- realms.create(github, RealmFields(githubName, githubOpenId, None, None))
          _ <- realms.create(gitlab, RealmFields(gitlabName, gitlabOpenId, None, None))
          _ <- realms.deprecate(gitlab, 1)
        } yield ()
      }
    }
  )

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie, seedRealms)

  test("Resolve the active realm and its jwks by issuer, then serve subsequent calls from the cache") {
    for {
      calls    <- Ref[IO].of(0)
      resolver <- RealmResolver(realms, countingResolver(calls, List(githubWk)), cacheConfig)
      resolved <- resolver(github.value)
      _         = assertEquals(resolved.map(_.label), Some(github))
      _        <- resolver(github.value)
      _        <- resolver(github.value)
      _        <- calls.get.assertEquals(1)
    } yield ()
  }

  test("Not resolve a deprecated realm") {
    for {
      calls    <- Ref[IO].of(0)
      resolver <- RealmResolver(realms, countingResolver(calls, List(githubWk)), cacheConfig)
      _        <- resolver(gitlab.value).assertEquals(None)
    } yield ()
  }

  test("Not resolve an unknown issuer") {
    for {
      calls    <- Ref[IO].of(0)
      resolver <- RealmResolver(realms, countingResolver(calls, List(githubWk)), cacheConfig)
      _        <- resolver("unknown").assertEquals(None)
    } yield ()
  }

  test("Refresh re-fetches the jwks and overwrites the cached entry") {
    val rotatedWk = githubWk.copy(keys = Set(rotatedKey))
    for {
      calls    <- Ref[IO].of(0)
      resolver <- RealmResolver(realms, countingResolver(calls, List(githubWk, rotatedWk)), cacheConfig)
      resolved <- resolver(github.value)
      active    = resolved.getOrElse(fail("the github realm should have been resolved"))
      _         = assertEquals(active.jwks.getKeys.size, 0) // initial stubbed key isn't a valid JWK
      _        <- resolver.refresh(active)
      // Subsequent apply call should be served from the cache, with no new fetch
      _        <- resolver(github.value)
      _        <- calls.get.assertEquals(2)
    } yield ()
  }

  test("Surface errors raised by the underlying WellKnownResolver") {
    val failing: WellKnownResolver = (uri: Uri) => IO.raiseError(UnsuccessfulOpenIdConfigResponse(uri))
    for {
      resolver <- RealmResolver(realms, failing, cacheConfig)
      _        <- resolver(github.value).interceptEquals(UnsuccessfulOpenIdConfigResponse(githubOpenId))
    } yield ()
  }

  // RSA keys returned by the resolver should all end up in the resulting JWKSet
  test("Build a JWKSet that contains all valid RSA keys returned by the resolver") {
    import com.nimbusds.jose.jwk.gen.RSAKeyGenerator
    val rsa1                        = new RSAKeyGenerator(2048).keyID("k1").generate().toPublicJWK
    val rsa2                        = new RSAKeyGenerator(2048).keyID("k2").generate().toPublicJWK
    val keysJson: Set[Json]         = Set(rsa1, rsa2).map(k => io.circe.parser.parse(k.toJSONString).toOption.get)
    val wkWithRsa: WellKnown        = githubWk.copy(keys = keysJson)
    val resolver: WellKnownResolver = (_: Uri) => IO.pure(wkWithRsa)
    for {
      realmResolver <- RealmResolver(realms, resolver, cacheConfig)
      resolved      <- realmResolver(github.value)
      _              = assertEquals(resolved.map(_.jwks.getKeys.size), Some(2))
    } yield ()
  }
}
