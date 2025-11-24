package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.generators.WellKnownGen
import ai.senscience.nexus.delta.sdk.model.Name
import ai.senscience.nexus.delta.sdk.realms.model.RealmFields
import ai.senscience.nexus.delta.sdk.realms.{RealmsConfig, RealmsImpl, RealmsProvisioningConfig, WellKnownResolver}
import ai.senscience.nexus.delta.sdk.realms.model.RealmRejection.UnsuccessfulOpenIdConfigResponse
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import munit.AnyFixture

class FetchRealmByIssuerSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private val (github, gitlab)         = (Label.unsafe("github"), Label.unsafe("gitlab"))
  private val githubName               = Name.unsafe("github-name")
  private val gitlabName               = Name.unsafe("gitlab-name")
  private val (githubOpenId, githubWk) = WellKnownGen.create(github.value)
  private val (gitlabOpenId, gitlabWk) = WellKnownGen.create(gitlab.value)

  private val wkResolver: WellKnownResolver = {
    case `githubOpenId` => IO.pure(githubWk)
    case `gitlabOpenId` => IO.pure(gitlabWk)
    case other          => IO.raiseError(UnsuccessfulOpenIdConfigResponse(other))
  }

  private val provisioning = RealmsProvisioningConfig(enabled = false, Map.empty)
  private val config       = RealmsConfig(eventLogConfig, pagination, provisioning)
  private lazy val realms  = RealmsImpl(config, wkResolver, xas, clock)

  test("Fetch the realms by issuer should work as expected") {
    given Subject         = Identity.User("user", Label.unsafe("realm"))
    val githubRealmFields = RealmFields(githubName, githubOpenId, None, None)
    val gitlabRealmFields = RealmFields(gitlabName, gitlabOpenId, None, None)
    for {
      _                  <- realms.create(github, githubRealmFields)
      githubRealm        <- realms.fetch(github).map(_.value)
      _                  <- realms.create(gitlab, gitlabRealmFields)
      _                  <- realms.deprecate(gitlab, 1)
      fetchRealmByIssuer <- FetchRealmByIssuer(realms, cacheConfig)
      // Github should be returned
      _                  <- fetchRealmByIssuer(github.value).assertEquals(Some(githubRealm))
      // Gitlab should not be returned as it is deprecated
      _                  <- fetchRealmByIssuer(gitlab.value).assertEquals(None)
    } yield ()
  }

}
