package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.cache.LocalCache
import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.*
import ai.senscience.nexus.delta.kernel.jwt.{AuthToken, ParsedToken}
import ai.senscience.nexus.delta.sdk.generators.{RealmGen, WellKnownGen}
import ai.senscience.nexus.delta.sdk.identities.IdentitiesImpl.{GroupsCache, RealmCache}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.realms.model.Realm
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Authenticated, Group, User}
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.ce.IOFromMap
import ai.senscience.nexus.testkit.jwt.TokenGenerator
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptySet
import cats.effect.{IO, Ref}
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jose.jwk.RSAKey
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator
import com.nimbusds.jwt.{JWTClaimsSet, PlainJWT}
import io.circe.{parser, Json}
import org.http4s.client.UnexpectedStatus
import org.http4s.{Credentials, Method, Status, Uri}

import java.time.Instant
import java.util.Date

class IdentitiesImplSuite extends NexusSuite with IOFromMap {

  /**
    * Generate RSA key
    */
  def generateKeys: RSAKey =
    new RSAKeyGenerator(2048)
      .keyID(genString())
      .generate()

  private val nowMinus1h = Instant.now().minusSeconds(3600)
  private val nowPlus1h  = Instant.now().plusSeconds(3600)

  private val rsaKey     = generateKeys
  private val signer     = new RSASSASigner(rsaKey.toPrivateKey)
  private val publicKeys = Set(parser.parse(rsaKey.toPublicJWK.toJSONString).rightValue)

  /**
    * Generate token
    */
  def generateToken(
      subject: String,
      issuer: Label,
      rsaKey: RSAKey = rsaKey,
      expires: Instant = nowPlus1h,
      notBefore: Instant = nowMinus1h,
      aud: Option[NonEmptySet[String]] = None,
      groups: Option[Set[String]] = None,
      useCommas: Boolean = false,
      preferredUsername: Option[String] = None
  ): AuthToken = TokenGenerator.generateToken(
    subject,
    issuer.value,
    rsaKey,
    expires,
    notBefore,
    aud,
    groups,
    useCommas,
    preferredUsername
  )

  private val githubLabel                = Label.unsafe("github")
  private val githubLabel2               = Label.unsafe("github2")
  private val (githubOpenId, githubWk)   = WellKnownGen.create(githubLabel.value)
  private val (githubOpenId2, githubWk2) = WellKnownGen.create(githubLabel2.value)

  private val github = RealmGen
    .realm(githubOpenId, githubWk)
    .copy(keys = publicKeys)

  private val github2 = RealmGen
    .realm(githubOpenId2, githubWk2, acceptedAudiences = Some(NonEmptySet.of("audience", "ba")))
    .copy(keys = publicKeys)

  private val gitlabLabel              = Label.unsafe("gitlab")
  private val (gitlabOpenId, gitlabWk) = WellKnownGen.create(gitlabLabel.value)

  private val gitlab = RealmGen
    .realm(gitlabOpenId, gitlabWk)
    .copy(
      keys = Set(parser.parse(rsaKey.toPublicJWK.toJSONString).rightValue)
    )

  type FindRealm = String => IO[Option[Realm]]

  private val findActiveRealm: String => IO[Option[Realm]] = ioFromMap[String, Realm](
    githubLabel.value  -> github,
    githubLabel2.value -> github2,
    gitlabLabel.value  -> gitlab
  )

  private def userInfo(uri: Uri): IO[Json] =
    ioFromMap(
      Map(github.userInfoEndpoint -> json"""{ "groups": ["group3", "group4"] }"""),
      (uri: Uri) => UnexpectedStatus(Status.InternalServerError, Method.GET, uri)
    )(uri)

  private val realmCache  = LocalCache[String, Realm]()
  private val groupsCache = LocalCache[String, Set[Group]]()

  private val identitiesFromCaches: (RealmCache, GroupsCache) => FindRealm => Identities =
    (realmCache, groupsCache) =>
      findRealm =>
        new IdentitiesImpl(
          realmCache,
          findRealm,
          (uri: Uri, _: Credentials.Token) => userInfo(uri),
          groupsCache
        )

  private val identities =
    identitiesFromCaches(realmCache.unsafeRunSync(), groupsCache.unsafeRunSync())(findActiveRealm)

  private val auth   = Authenticated(githubLabel)
  private val group1 = Group("group1", githubLabel)
  private val group2 = Group("group2", githubLabel)
  private val group3 = Group("group3", githubLabel)
  private val group4 = Group("group4", githubLabel)

  test("Successfully extract the caller") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel,
      rsaKey = rsaKey,
      expires = nowPlus1h,
      groups = Some(Set("group1", "group2")),
      preferredUsername = Some("Bob")
    )

    val user     = User("Bob", githubLabel)
    val expected = Caller(user, Set(user, Anonymous, auth, group1, group2))
    identities.exchange(token).assertEquals(expected)
  }

  test("Succeed when the token is valid and preferred user name is not set") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel,
      rsaKey = rsaKey,
      expires = nowPlus1h,
      groups = Some(Set("group1", "group2"))
    )

    val user     = User("Robert", githubLabel)
    val expected = Caller(user, Set(user, Anonymous, auth, group1, group2))
    identities.exchange(token).assertEquals(expected)
  }

  test("Succeed when the token is valid and groups are comma delimited") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel,
      rsaKey = rsaKey,
      expires = nowPlus1h,
      groups = Some(Set("group1", "group2")),
      useCommas = true
    )

    val user     = User("Robert", githubLabel)
    val expected = Caller(user, Set(user, Anonymous, auth, group1, group2))
    identities.exchange(token).assertEquals(expected)
  }

  test("Succeed when the token is valid and groups are defined") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel,
      rsaKey = rsaKey,
      expires = nowPlus1h,
      groups = None,
      useCommas = true
    )

    val user     = User("Robert", githubLabel)
    val expected = Caller(user, Set(user, Anonymous, auth, group3, group4))
    identities.exchange(token).assertEquals(expected)
  }

  test("Succeed when the token is valid and aud matches the available audiences") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel2,
      rsaKey = rsaKey,
      expires = nowPlus1h,
      aud = Some(NonEmptySet.of("ca", "ba")),
      groups = Some(Set("group1", "group2"))
    )

    val user     = User("Robert", githubLabel2)
    val group1   = Group("group1", githubLabel2)
    val group2   = Group("group2", githubLabel2)
    val expected = Caller(user, Set(user, Anonymous, Authenticated(githubLabel2), group1, group2))
    identities.exchange(token).assertEquals(expected)
  }

  test("Fail when the token is valid but aud does not match the available audiences") {
    val token         = generateToken(
      subject = "Robert",
      issuer = githubLabel2,
      rsaKey = rsaKey,
      expires = nowPlus1h,
      aud = Some(NonEmptySet.of("ca", "de")),
      groups = Some(Set("group1", "group2"))
    )
    val expectedError = InvalidAccessToken("Robert", githubLabel2.value, "JWT audience rejected: [ca, de]")
    identities.exchange(token).interceptEquals(expectedError)
  }

  test("Fail when the token is invalid") {
    identities.exchange(AuthToken(genString())).intercept[InvalidAccessTokenFormat]
  }

  test("Fail when the token is not signed") {
    val csb = new JWTClaimsSet.Builder()
      .subject("subject")
      .expirationTime(Date.from(nowPlus1h))

    val token = AuthToken(new PlainJWT(csb.build()).serialize())
    identities.exchange(token).intercept[InvalidAccessTokenFormat]
  }

  test("Fail when the token doesn't contain an issuer") {
    val csb = new JWTClaimsSet.Builder()
      .subject("subject")
      .expirationTime(Date.from(nowPlus1h))

    val token = TokenGenerator.toSignedJwt(csb, rsaKey, signer)
    identities.exchange(token).interceptEquals(AccessTokenDoesNotContainAnIssuer)
  }

  test("Fail when the token doesn't contain a subject") {
    val csb = new JWTClaimsSet.Builder()
      .issuer(githubLabel.value)
      .expirationTime(Date.from(nowPlus1h))

    val token = TokenGenerator.toSignedJwt(csb, rsaKey, signer)
    identities.exchange(token).interceptEquals(AccessTokenDoesNotContainSubject)
  }

  test("Fail when the token doesn't contain a known issuer") {
    val token = generateToken(
      subject = "Robert",
      issuer = Label.unsafe("unknown"),
      rsaKey = rsaKey,
      groups = None,
      useCommas = true
    )

    identities.exchange(token).interceptEquals(UnknownAccessTokenIssuer)
  }

  test("Fail when the token is expired") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel,
      rsaKey = rsaKey,
      expires = nowMinus1h,
      groups = None,
      useCommas = true
    )

    val expectedError = InvalidAccessToken("Robert", githubLabel.value, "Expired JWT")
    identities.exchange(token).interceptEquals(expectedError)
  }

  test("Fail when the token is not yet valid") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel,
      rsaKey = rsaKey,
      notBefore = nowPlus1h,
      groups = None,
      useCommas = true
    )

    val expectedError = InvalidAccessToken("Robert", githubLabel.value, "JWT before use time")
    identities.exchange(token).interceptEquals(expectedError)
  }

  test("Fail when the signature is invalid") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel,
      rsaKey = generateKeys,
      groups = None,
      useCommas = true
    )

    val expectedError = InvalidAccessToken(
      "Robert",
      githubLabel.value,
      "Signed JWT rejected: Another algorithm expected, or no matching key(s) found"
    )
    identities.exchange(token).interceptEquals(expectedError)
  }

  test("Fail when getting groups from the oidc provider can't be complete") {
    val token = generateToken(
      subject = "Robert",
      issuer = gitlabLabel,
      rsaKey = rsaKey,
      groups = None,
      useCommas = true
    )

    val expectedError = GetGroupsFromOidcError("Robert", gitlabLabel.value)
    identities.exchange(token).interceptEquals(expectedError)
  }

  test("Cache realm and groups") {
    val token = generateToken(
      subject = "Bobby",
      issuer = githubLabel,
      rsaKey = rsaKey,
      expires = nowPlus1h,
      groups = None,
      useCommas = true
    )

    for {
      parsedToken <- IO.fromEither(ParsedToken.fromToken(token))
      realm       <- realmCache
      groups      <- groupsCache
      _           <- realm.get(parsedToken.rawToken).assertEquals(None)
      _           <- groups.get(parsedToken.rawToken).assertEquals(None)
      _           <- identitiesFromCaches(realm, groups)(findActiveRealm).exchange(token)
      _           <- realm.get(parsedToken.issuer).assertEquals(Some(github))
      _           <- groups.get(parsedToken.rawToken).assertEquals(Some(Set(group3, group4)))
    } yield ()
  }

  test("Find active realm function should not run once value is cached") {
    val token = generateToken(
      subject = "Robert",
      issuer = githubLabel,
      rsaKey = rsaKey,
      expires = nowPlus1h,
      groups = Some(Set("group1", "group2"))
    )

    def findRealmOnce: Ref[IO, Boolean] => String => IO[Option[Realm]] = ref =>
      _ =>
        for {
          flag <- ref.get
          _    <- IO.raiseWhen(!flag)(new RuntimeException("Function executed more than once!"))
          _    <- ref.set(false)
        } yield Some(github)

    for {
      sem       <- Ref.of[IO, Boolean](true)
      realm     <- realmCache
      groups    <- groupsCache
      identities = identitiesFromCaches(realm, groups)(findRealmOnce(sem))
      _         <- identities.exchange(token)
      _         <- identities.exchange(token)
    } yield ()
  }

}
