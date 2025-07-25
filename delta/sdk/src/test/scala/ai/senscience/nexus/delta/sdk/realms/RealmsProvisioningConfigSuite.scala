package ai.senscience.nexus.delta.sdk.realms

import ai.senscience.nexus.delta.sdk.model.Name
import ai.senscience.nexus.delta.sdk.realms.model.RealmFields
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptySet
import org.http4s.implicits.http4sLiteralsSyntax
import pureconfig.ConfigSource

class RealmsProvisioningConfigSuite extends NexusSuite {

  private def parseConfig(value: String) =
    ConfigSource.string(value).at("provisioning").load[RealmsProvisioningConfig]

  test("Parse successfully the config when no realm is configured") {
    val obtained = parseConfig(
      """
        |provisioning {
        |  enabled = false
        |  realms {
        |  }
        |}
        |""".stripMargin
    )

    val expected = RealmsProvisioningConfig(enabled = false, Map.empty)
    assertEquals(obtained, Right(expected))
  }

  test("Parse successfully the config is enable with several realms") {
    val obtained = parseConfig(
      """
        |provisioning {
        |  enabled = true
        |  realms {
        |    bbp = {
        |     name = "BBP"
        |     open-id-config = "https://bbp.epfl.ch/path/.well-known/openid-configuration"
        |     logo = "https://bbp.epfl.ch/path/favicon.png"
        |     accepted-audiences = ["audience1", "audience2"]
        |    }
        |    obp = {
        |     name = "OBP"
        |     open-id-config = "https://openbluebrain.com/path/.well-known/openid-configuration"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val bbp = RealmFields(
      Name.unsafe("BBP"),
      uri"https://bbp.epfl.ch/path/.well-known/openid-configuration",
      Some(uri"https://bbp.epfl.ch/path/favicon.png"),
      Some(NonEmptySet.of("audience1", "audience2"))
    )

    val obp = RealmFields(
      Name.unsafe("OBP"),
      uri"https://openbluebrain.com/path/.well-known/openid-configuration",
      None,
      None
    )

    val expected = RealmsProvisioningConfig(
      enabled = true,
      Map(
        Label.unsafe("bbp") -> bbp,
        Label.unsafe("obp") -> obp
      )
    )
    assertEquals(obtained, Right(expected))
  }

}
