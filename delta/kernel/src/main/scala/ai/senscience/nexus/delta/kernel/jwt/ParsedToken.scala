package ai.senscience.nexus.delta.kernel.jwt

import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.{AccessTokenDoesNotContainAnIssuer, AccessTokenDoesNotContainSubject, InvalidAccessToken, InvalidAccessTokenFormat}
import ai.senscience.nexus.delta.kernel.syntax.*
import cats.data.NonEmptySet
import cats.syntax.all.*
import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.jwk.JWKSet
import com.nimbusds.jose.jwk.source.ImmutableJWKSet
import com.nimbusds.jose.proc.{JWSVerificationKeySelector, SecurityContext}
import com.nimbusds.jwt.proc.{DefaultJWTClaimsVerifier, DefaultJWTProcessor}
import com.nimbusds.jwt.{JWTClaimsSet, SignedJWT}

import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.util.Try

/**
  * Token where we extracted and validated the information needed from the [[jwtToken]]
  */
final case class ParsedToken private (
    rawToken: String,
    subject: String,
    issuer: String,
    expirationTime: Instant,
    roles: Option[Set[String]],
    groups: Option[Set[String]],
    jwtToken: SignedJWT
) {

  def validate(audiences: Option[NonEmptySet[String]], keySet: JWKSet): Either[InvalidAccessToken, Unit] = {
    val proc        = new DefaultJWTProcessor[SecurityContext]
    val keySelector = new JWSVerificationKeySelector(JWSAlgorithm.RS256, new ImmutableJWKSet[SecurityContext](keySet))
    proc.setJWSKeySelector(keySelector)
    audiences.foreach { aud =>
      proc.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier(aud.toSet.asJava, null, null, null))
    }
    Either
      .catchNonFatal(proc.process(jwtToken, null))
      .bimap(
        err => InvalidAccessToken(subject, issuer, err.getMessage),
        _ => ()
      )
  }

}

object ParsedToken {

  /**
    * Parse token and try to extract expected information from it
    *
    * @param token
    *   the raw token
    */
  def fromToken(token: AuthToken): Either[TokenRejection, ParsedToken] = {

    def parseJwt: Either[TokenRejection, SignedJWT] =
      Either
        .catchNonFatal(SignedJWT.parse(token.value))
        .leftMap { e => InvalidAccessTokenFormat(e.getMessage) }

    def claims(jwt: SignedJWT): Either[TokenRejection, JWTClaimsSet] =
      Either
        .catchNonFatal(Option(jwt.getJWTClaimsSet))
        .leftMap { e => InvalidAccessTokenFormat(e.getMessage) }
        .flatMap { _.toRight(InvalidAccessTokenFormat("No claim is defined.")) }

    def subject(claimsSet: JWTClaimsSet) = {
      val preferredUsername = Try(claimsSet.getStringClaim("preferred_username"))
        .filter(_ != null)
        .toOption
      (preferredUsername orElse Option(claimsSet.getSubject)).toRight(AccessTokenDoesNotContainSubject)
    }

    def issuer(claimsSet: JWTClaimsSet): Either[TokenRejection, String] =
      Option(claimsSet.getIssuer).toRight(AccessTokenDoesNotContainAnIssuer)

    def roles(claimsSet: JWTClaimsSet): Option[Set[String]] =
      Option.when(
        claimsSet.getClaims.containsKey("roles")
      )(getStringListClaim(claimsSet, "roles").getOrElse(Set.empty))

    def groups(claimsSet: JWTClaimsSet): Option[Set[String]] =
      Option.when(
        claimsSet.getClaims.containsKey("groups")
      ) {
        getStringListClaim(claimsSet, "groups")
          .orElse(
            Try(claimsSet.getStringClaim("groups").split(",").map(_.trim).toSet).toOption
          )
          .getOrElse(Set.empty)
      }

    def getStringListClaim(claimsSet: JWTClaimsSet, name: String) =
      Try(claimsSet.getStringListClaim(name).asScala.toSet)
        .filter(_ != null)
        .map(_.map(_.trim))
        .map(_.filterNot(_.isEmpty))
        .toOption

    for {
      jwt           <- parseJwt
      claimsSet     <- claims(jwt)
      subject       <- subject(claimsSet)
      issuer        <- issuer(claimsSet)
      expirationTime = claimsSet.getExpirationTime.toInstant
      roleSet        = roles(claimsSet)
      groupSet       = groups(claimsSet)
    } yield ParsedToken(token.value, subject, issuer, expirationTime, roleSet, groupSet, jwt)
  }
}
