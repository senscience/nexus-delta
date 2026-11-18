package ai.senscience.nexus.delta.kernel.http.client.middleware

import ai.senscience.nexus.delta.kernel.Secret
import cats.effect.{IO, Ref, Resource}
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.client.Client
import org.http4s.syntax.all.*
import org.typelevel.ci.*

class HttpAuthSuite extends CatsEffectSuite {

  // Runs a request through a client wrapped with `auth` and captures the raw `Authorization` header it added.
  private def authHeader(
      auth: HttpAuth,
      request: Request[IO] = Request[IO](Method.GET, uri"http://localhost/test")
  ): IO[Option[String]] =
    Ref.of[IO, Option[String]](None).flatMap { ref =>
      val underlying = Client[IO] { request =>
        Resource
          .eval(ref.set(request.headers.get(ci"Authorization").map(_.head.value)))
          .map(_ => Response[IO]())
      }
      HttpAuth(auth)(underlying).run(request).use_ >> ref.get
    }

  test("Anonymous leaves the request without an Authorization header") {
    authHeader(HttpAuth.Anonymous).assertEquals(None)
  }

  test("Basic adds a Basic Authorization header") {
    authHeader(HttpAuth.Basic(Secret(BasicCredentials("user", "pass"))))
      .assertEquals(Some("Basic dXNlcjpwYXNz"))
  }

  test("ApiKey adds an ApiKey Authorization header") {
    authHeader(HttpAuth.ApiKey(Secret(Credentials.Token(ci"ApiKey", "abc123"))))
      .assertEquals(Some("ApiKey abc123"))
  }

  test("Basic keeps an Authorization header already set on the request") {
    val request = Request[IO](Method.GET, uri"http://localhost/test")
      .putHeaders(headers.Authorization(Credentials.Token(AuthScheme.Bearer, "token")))
    authHeader(HttpAuth.Basic(Secret(BasicCredentials("user", "pass"))), request)
      .assertEquals(Some("Bearer token"))
  }
}
