package ai.senscience.nexus.delta.sdk.auth

import ai.senscience.nexus.delta.kernel.http.circe.*
import ai.senscience.nexus.delta.kernel.jwt.{AuthToken, ParsedToken}
import ai.senscience.nexus.delta.kernel.{Logger, Secret}
import ai.senscience.nexus.delta.sdk.auth.Credentials.ClientCredentials
import ai.senscience.nexus.delta.sdk.auth.OpenIdAuthService.logger
import ai.senscience.nexus.delta.sdk.error.AuthTokenError.{AuthTokenHttpError, AuthTokenNotFoundInResponse, RealmIsDeprecated}
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.realms.model.Realm
import ai.senscience.nexus.delta.sourcing.model.Label
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import org.http4s.Method.POST
import org.http4s.client.Client
import org.http4s.client.dsl.io.*
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, Uri, UrlForm}

/**
  * Exchanges client credentials for an auth token with a remote OpenId service, as defined in the specified realm
  */
class OpenIdAuthService(client: Client[IO], realms: Realms) {

  private val urlForm = UrlForm(
    "scope"      -> "openid",
    "grant_type" -> "client_credentials"
  )

  /**
    * Exchanges client credentials for an auth token with a remote OpenId service, as defined in the specified realm
    */
  def auth(credentials: ClientCredentials): IO[ParsedToken] = {
    for {
      realm       <- findRealm(credentials.realm)
      response    <- requestToken(realm.tokenEndpoint, credentials.user, credentials.password)
      parsedToken <- IO.fromEither(parseResponse(response))
    } yield {
      parsedToken
    }
  }

  private def findRealm(id: Label): IO[Realm] =
    realms.fetch(id).flatMap { realm =>
      IO.raiseWhen(realm.deprecated)(RealmIsDeprecated(realm.value)).as(realm.value)
    }

  private def requestToken(tokenEndpoint: Uri, user: String, password: Secret[String]): IO[Json] = {
    val request = POST(tokenEndpoint, Authorization(BasicCredentials(user, password.value)))
      .withEntity(urlForm)
    client.expectOr[Json](request) { response =>
      response.bodyAsString.flatMap { body =>
        val error = AuthTokenHttpError(response.status)
        logger
          .error(s"The token could not be retrieved. The service returned: ${response.status} => $body")
          .as(error)
      }
    }
  }

  private def parseResponse(json: Json) =
    json.hcursor
      .get[String]("access_token")
      .leftMap(AuthTokenNotFoundInResponse)
      .flatMap { rawToken =>
        ParsedToken.fromToken(AuthToken(rawToken))
      }
}

object OpenIdAuthService {
  private val logger = Logger[this.type]
}
