package ai.senscience.nexus.delta.sdk.realms

import ai.senscience.nexus.delta.kernel.http.circe.CirceInstances
import ai.senscience.nexus.delta.rdf.syntax.JsonSyntax
import ai.senscience.nexus.delta.sdk.realms.model.GrantType
import ai.senscience.nexus.delta.sdk.realms.model.GrantType.*
import ai.senscience.nexus.delta.sdk.realms.model.RealmRejection.{IllegalEndpointFormat, IllegalIssuerFormat, IllegalJwkFormat, IllegalJwksUriFormat, NoValidKeysFound, UnsuccessfulJwksResponse, UnsuccessfulOpenIdConfigResponse}
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.ce.IOFromMap
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator
import io.circe.Json
import io.circe.syntax.KeyOps
import org.http4s.client.UnexpectedStatus
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Method, Status, Uri}

class WellKnownResolverSuite extends NexusSuite with IOFromMap with CirceLiteral with CirceInstances with JsonSyntax {

  private val openIdUri = uri"https://localhost/auth/realms/master/.well-known/openid-configuration"
  private val jwksUri   = uri"https://localhost/auth/realms/master/protocol/openid-connect/certs"
  private val issuer    = "https://localhost/auth/realms/master"

  private val authorizationUri = uri"https://localhost/auth"
  private val tokenUri         = uri"https://localhost/auth/token"
  private val userInfoUri      = uri"https://localhost/auth/userinfo"
  private val revocationUri    = uri"https://localhost/auth/revoke"
  private val endSessionUri    = uri"https://localhost/auth/logout"

  private val publicKey =
    new RSAKeyGenerator(2048)
      .keyID("123")
      .generate()
      .toPublicJWK
      .toJSONString

  private val publicKeyJson = json"$publicKey"

  private val validJwks = json"""{ "keys": [ $publicKey ] }"""

  private val emitError = (uri: Uri) => UnexpectedStatus(Status.InternalServerError, Method.GET, uri)

  private def resolveWellKnown(openIdConfig: Json, jwks: Json) =
    WellKnownResolver(
      ioFromMap(
        Map(
          openIdUri -> openIdConfig,
          jwksUri   -> jwks
        ),
        emitError
      )
    )(openIdUri)

  private val defaultConfig =
    json"""
      {
        "issuer": "$issuer",
        "jwks_uri": "$jwksUri",
        "grant_types_supported": [
          "authorization_code",
          "implicit",
          "refresh_token",
          "password",
          "client_credentials"
        ],
        "authorization_endpoint": "$authorizationUri",
        "token_endpoint": "$tokenUri",
        "userinfo_endpoint": "$userInfoUri"
      }
    """

  private val fullConfig = defaultConfig deepMerge Json.obj(
    "revocation_endpoint"  := revocationUri,
    "end_session_endpoint" := endSessionUri
  )

  test("Succeed with the expected grant types") {
    val config = defaultConfig

    val expectedGrantTypes = Set(AuthorizationCode, Implicit, RefreshToken, Password, ClientCredentials)
    resolveWellKnown(
      config,
      validJwks
    ).map { wk =>
      assertEquals(wk.issuer, issuer)
      assertEquals(wk.grantTypes, expectedGrantTypes)
      assertEquals(wk.keys, Set(publicKeyJson))
    }
  }

  test("Succeed with empty grant types") {
    val emptyGrantTypes = defaultConfig deepMerge Json.obj("grant_types_supported" -> Json.arr())
    resolveWellKnown(
      emptyGrantTypes,
      validJwks
    ).map { wk =>
      assertEquals(wk.grantTypes, Set.empty[GrantType])
    }
  }

  test("Succeed with no grant types field") {
    val noGrantTypes = defaultConfig.removeKeys("grant_types_supported")
    resolveWellKnown(
      noGrantTypes,
      validJwks
    ).map { wk =>
      assertEquals(wk.grantTypes, Set.empty[GrantType])
    }
  }

  test("Populate the different endpoints") {
    resolveWellKnown(
      fullConfig,
      validJwks
    ).map { wk =>
      assertEquals(wk.authorizationEndpoint, authorizationUri)
      assertEquals(wk.tokenEndpoint, tokenUri)
      assertEquals(wk.userInfoEndpoint, userInfoUri)
      assertEquals(wk.revocationEndpoint, Some(revocationUri))
      assertEquals(wk.endSessionEndpoint, Some(endSessionUri))
    }
  }

  test("Fail if the client returns a bad response") {
    val alwaysFail = WellKnownResolver(uri => IO.raiseError(emitError(uri)))(openIdUri)
    alwaysFail.interceptEquals(UnsuccessfulOpenIdConfigResponse(openIdUri))
  }

  test("Fail if the openid contains an invalid issuer") {
    val invalidIssuer = defaultConfig deepMerge Json.obj("issuer" := " ")

    resolveWellKnown(
      invalidIssuer,
      validJwks
    ).interceptEquals(IllegalIssuerFormat(openIdUri, ".issuer"))
  }

  test("Fail if the openid contains an issuer with an invalid type") {
    val invalidIssuer = defaultConfig deepMerge Json.obj("issuer" := 42)

    resolveWellKnown(
      invalidIssuer,
      validJwks
    ).interceptEquals(IllegalIssuerFormat(openIdUri, ".issuer"))
  }

  test("Fail if the openid contains an issuer with an invalid type") {
    val invalidIssuer = defaultConfig deepMerge Json.obj("jwks_uri" := "asd")

    resolveWellKnown(
      invalidIssuer,
      validJwks
    ).interceptEquals(IllegalJwksUriFormat(openIdUri, ".jwks_uri"))
  }

  List(
    "authorization_endpoint",
    "token_endpoint",
    "userinfo_endpoint",
    "revocation_endpoint",
    "end_session_endpoint"
  ).foreach { key =>
    test(s"Fail if the openid contains an invalid '$key' endpoint") {
      val invalidEndpoint = fullConfig deepMerge Json.obj(key := 42)
      resolveWellKnown(
        invalidEndpoint,
        validJwks
      ).interceptEquals(IllegalEndpointFormat(openIdUri, s".$key"))
    }
  }

  List("authorization_endpoint", "token_endpoint", "userinfo_endpoint").foreach { key =>
    test(s"Fail if the openid does not contain the '$key' endpoint") {
      val invalidEndpoint = fullConfig.removeKeys(key)
      resolveWellKnown(
        invalidEndpoint,
        validJwks
      ).interceptEquals(IllegalEndpointFormat(openIdUri, s".$key"))
    }
  }

  test("Fail if there is a bad response for the jwks document") {
    val invalidJwksUri = uri"https://localhost/invalid"
    val invalidJwks    = defaultConfig deepMerge Json.obj("jwks_uri" := invalidJwksUri)

    resolveWellKnown(
      invalidJwks,
      validJwks
    ).interceptEquals(UnsuccessfulJwksResponse(invalidJwksUri))
  }

  test("Fail if the jwks document has an incorrect format") {
    resolveWellKnown(
      defaultConfig,
      Json.obj()
    ).interceptEquals(IllegalJwkFormat(jwksUri))
  }

  test("Fail if the jwks document has an incorrect format") {
    resolveWellKnown(
      defaultConfig,
      Json.obj()
    ).interceptEquals(IllegalJwkFormat(jwksUri))
  }

  test("Fail if the jwks document has no keys") {
    resolveWellKnown(
      defaultConfig,
      Json.obj("keys" -> Json.arr())
    ).interceptEquals(NoValidKeysFound(jwksUri))
  }

  test("Fail if the jwks document has no valid keys") {
    resolveWellKnown(
      defaultConfig,
      Json.obj("keys" -> Json.arr(Json.fromString("incorrect")))
    ).interceptEquals(NoValidKeysFound(jwksUri))
  }

}
