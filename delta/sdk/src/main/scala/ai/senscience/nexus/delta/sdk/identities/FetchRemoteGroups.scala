package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.cache.{CacheConfig, LocalCache}
import ai.senscience.nexus.delta.kernel.http.ResponseUtils.decodeBodyAsJson
import ai.senscience.nexus.delta.kernel.http.circe.CirceEntityDecoder.given
import ai.senscience.nexus.delta.kernel.jwt.ParsedToken
import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.{GetGroupsFromOidcError, InvalidAccessToken}
import cats.effect.IO
import io.circe.Decoder
import org.http4s.Method.GET
import org.http4s.client.Client
import org.http4s.client.dsl.io.*
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, Status, Uri}

trait FetchRemoteGroups {
  def apply(userEndpoint: Uri, token: ParsedToken): IO[Set[String]]
}

object FetchRemoteGroups {

  private val logger = Logger[FetchRemoteGroups]

  final private case class UserInfo(groups: Set[String])

  private object UserInfo {
    given Decoder[UserInfo] = Decoder.decodeHCursor.map { cursor =>
      val fromSet = cursor.get[Set[String]]("groups").map(_.map(_.trim).filterNot(_.isEmpty))
      val fromCsv = cursor.get[String]("groups").map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      UserInfo(fromSet.orElse(fromCsv).getOrElse(Set.empty))
    }
  }

  private object Noop extends FetchRemoteGroups {
    override def apply(userEndpoint: Uri, token: ParsedToken): IO[Set[String]] = IO.pure(Set.empty)
  }

  final private class Active(cache: LocalCache[String, UserInfo], client: Client[IO]) extends FetchRemoteGroups {

    override def apply(userEndpoint: Uri, token: ParsedToken): IO[Set[String]] =
      cache.getOrElseUpdate(token.rawToken, internalFetch(userEndpoint, token)).map { _.groups }

    private def internalFetch(userEndpoint: Uri, token: ParsedToken): IO[UserInfo] = {
      val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token.rawToken))
      val request    = GET(userEndpoint, authHeader)
      client.expectOr[UserInfo](request) {
        case response if response.status == Status.Unauthorized || response.status == Status.Forbidden =>
          decodeBodyAsJson(response).flatMap { body =>
            val message =
              s"A provided client token was rejected by the OIDC provider for user '${token.subject}' of realm '${token.issuer}', reason: '$body'"
            logger.debug(message) >> IO.raiseError(
              InvalidAccessToken(token.subject, token.issuer, response.status.reason)
            )
          }
        case response                                                                                  =>
          decodeBodyAsJson(response).flatMap { body =>
            val message =
              s"A call to get the groups from the OIDC provider failed unexpectedly for user '${token.subject}' of realm '${token.issuer}', reason: '$body'."
            logger.error(message) >> IO.raiseError(GetGroupsFromOidcError(token.subject, token.issuer))
          }
      }
    }
  }

  def apply(enabled: Boolean, client: Client[IO], cacheConfig: CacheConfig): IO[FetchRemoteGroups] =
    if enabled then
      logger.info("Fetching remote groups for user is enabled") >>
        LocalCache[String, UserInfo](cacheConfig).map { cache =>
          new Active(cache, client)
        }
    else logger.info("Fetching remote groups for user is disabled").as(Noop)

}
