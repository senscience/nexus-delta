package ai.senscience.nexus.delta.sdk.identities

import ai.senscience.nexus.delta.kernel.cache.CacheConfig
import ai.senscience.nexus.delta.kernel.http.circe.CirceEntityEncoder.given
import ai.senscience.nexus.delta.kernel.jwt.ParsedToken
import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.InvalidAccessToken
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.jwt.TokenGenerator
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator
import org.http4s.HttpApp
import org.http4s.client.*
import org.http4s.dsl.io.*
import org.http4s.headers.Authorization
import org.http4s.implicits.uri
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.time.Instant

class FetchRemoteGroupsSuite extends NexusSuite with CirceLiteral {

  private val routes = HttpApp[IO] {
    case req @ GET -> Root / "userinfo" / "set" if req.headers.contains[Authorization] =>
      Ok(json"""{ "groups":  ["group1", "group2" ] }""")
    case GET -> Root / "userinfo" / "csv"                                              =>
      Ok(json"""{ "groups": "group1, group2" }""")
    case _                                                                             => Forbidden(json"""{ "error": "fail"}""")
  }

  private val client = Client.fromHttpApp(routes)

  private val cacheConfig = CacheConfig(enabled = true, 10, 3.seconds)

  private val rsaKey = new RSAKeyGenerator(2048)
    .keyID(genString())
    .generate()

  private def generateToken = {
    val authToken = TokenGenerator.generateToken(
      "Robert",
      "github",
      rsaKey,
      Instant.now().plusSeconds(3600),
      Instant.now().minusSeconds(3600)
    )
    IO.fromEither(ParsedToken.fromToken(authToken))
  }

  test("Return an empty set of groups when disabled") {
    for {
      fetchRemoteGroups <- FetchRemoteGroups(enabled = false, client, cacheConfig)
      token             <- generateToken
      _                 <- fetchRemoteGroups(uri"/invalid", token).assertEquals(Set.empty)
    } yield ()
  }

  test("Return an error when enabled and endpoint fails") {
    for {
      fetchRemoteGroups <- FetchRemoteGroups(enabled = true, client, cacheConfig)
      token             <- generateToken
      _                 <- fetchRemoteGroups(uri"/invalid", token).intercept[InvalidAccessToken]
    } yield ()
  }

  test("Return the groups when they are provided as a set") {
    for {
      fetchRemoteGroups <- FetchRemoteGroups(enabled = true, client, cacheConfig)
      token             <- generateToken
      _                 <- fetchRemoteGroups(uri"/userinfo/set", token).assertEquals(Set("group1", "group2"))
    } yield ()
  }

  test("Return the groups when they are provided as a csv") {
    for {
      fetchRemoteGroups <- FetchRemoteGroups(enabled = true, client, cacheConfig)
      token             <- generateToken
      _                 <- fetchRemoteGroups(uri"/userinfo/csv", token).assertEquals(Set("group1", "group2"))
    } yield ()
  }

}
