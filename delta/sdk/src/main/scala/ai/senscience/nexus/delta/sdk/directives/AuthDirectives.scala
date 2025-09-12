package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.jwt.{AuthToken, TokenRejection}
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.error.IdentityError.{AuthenticationFailed, InvalidToken}
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import cats.syntax.all.*
import org.apache.pekko.http.scaladsl.model.headers.OAuth2BearerToken
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.server.directives.Credentials

import scala.concurrent.Future

/**
  * Pekko HTTP directives for authentication
  */
abstract class AuthDirectives(identities: Identities, aclCheck: AclCheck) extends Directives {

  private def authenticator: AsyncAuthenticator[Caller] = {
    case Credentials.Missing         => Future.successful(None)
    case Credentials.Provided(token) =>
      val cred = OAuth2BearerToken(token)
      identities
        .exchange(AuthToken(cred.token))
        .attemptNarrow[TokenRejection]
        .flatMap { attempt =>
          IO.fromEither(attempt.bimap(InvalidToken, Some(_)))
        }
        .unsafeToFuture()
  }

  private def isBearerToken: Directive0 =
    extractCredentials.flatMap {
      case Some(OAuth2BearerToken(_)) => pass
      case Some(_)                    => failWith(AuthenticationFailed)
      case _                          => pass
    }

  /**
    * Attempts to extract the Credentials from the HTTP call and generate a [[Caller]] from it.
    */
  def extractCaller: Directive1[Caller] =
    isBearerToken.tflatMap(_ => authenticateOAuth2Async("*", authenticator).withAnonymousUser(Caller.Anonymous))

  /**
    * Checks whether given [[Caller]] has the [[Permission]] on the [[AclAddress]].
    */
  def authorizeFor(path: AclAddress, permission: Permission)(implicit caller: Caller): Directive0 =
    authorizeAsync(aclCheck.authorizeFor(path, permission).unsafeToFuture()) or
      extractRequest.flatMap { request =>
        failWith(AuthorizationFailed(request, path, permission))
      }

  def authorizeForIO(path: AclAddress, fetchPermission: IO[Permission])(implicit caller: Caller): Directive0 =
    onSuccess(fetchPermission.unsafeToFuture()).flatMap { permission =>
      authorizeFor(path, permission)
    }

}
