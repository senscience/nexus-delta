package ai.senscience.nexus.delta.kernel.http.client.middleware

import ai.senscience.nexus.delta.kernel.Secret
import cats.effect.IO
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, Credentials}
import org.typelevel.ci.*
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

/**
  * Authentication method applied by [[HttpAuth.apply]] to the requests of an HTTP client.
  */
enum HttpAuth {
  case Anonymous
  case Basic(credentials: Secret[BasicCredentials])
  case ApiKey(token: Secret[Credentials.Token])
}

object HttpAuth {

  /**
    * Wraps the client so that every request carries the `Authorization` header derived from `auth`:
    */
  def apply(auth: HttpAuth)(client: Client[IO]): Client[IO] = {
    val authorization = auth match {
      case Anonymous     => None
      case Basic(creds)  => Some(Authorization(creds.value))
      case ApiKey(token) => Some(Authorization(token.value))
    }
    Client { request =>
      client.run(authorization.fold(request)(request.putHeaders(_)))
    }
  }

  /**
    * Reads an [[HttpAuth]] from a single block, discriminated by a required `type` key, so the auth methods are
    * mutually exclusive by construction: {{{{ type = "basic", username = "...", password = "..." }}}},
    * {{{{ type = "api-key", value = "..." }}}} or {{{{ type = "anonymous" }}}}.
    */
  given ConfigReader[HttpAuth] = ConfigReader.fromCursor { cursor =>
    cursor.asObjectCursor.flatMap { obj =>
      obj.atKey("type").flatMap(_.asString).flatMap {
        case "basic"     =>
          for {
            username <- obj.atKey("username").flatMap(_.asString)
            password <- obj.atKey("password").flatMap(_.asString)
          } yield Basic(Secret(BasicCredentials(username, password)))
        case "api-key"   =>
          obj.atKey("value").flatMap(_.asString).map(value => ApiKey(Secret(Credentials.Token(ci"ApiKey", value))))
        case "anonymous" => Right(Anonymous)
        case other       =>
          obj.failed[HttpAuth](CannotConvert(other, "HttpAuth", "'type' must be 'basic', 'api-key' or 'anonymous'"))
      }
    }
  }
}
