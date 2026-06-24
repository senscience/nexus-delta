package ai.senscience.nexus.delta.sdk.otel

import cats.effect.IO
import com.comcast.ip4s.{IpAddress, Port}
import munit.FunSuite
import org.http4s.{Header, Headers, Method, Request, Status, Uri}
import org.typelevel.ci.*
import org.typelevel.otel4s.semconv.attributes.*

class OtelHttp4sAttributesSuite extends FunSuite {

  private def port(value: Int): Port  = Port.fromInt(value).get
  private def uri(value: String): Uri = Uri.unsafeFromString(value)

  test("errorType from a throwable uses its class name") {
    assertEquals(
      OtelHttp4sAttributes.errorType(new IllegalStateException("boom")),
      ErrorAttributes.ErrorType("java.lang.IllegalStateException")
    )
  }

  test("errorType from a status: present for failures, absent for successes") {
    assertEquals(OtelHttp4sAttributes.errorType(Status.Ok), None)
    assertEquals(OtelHttp4sAttributes.errorType(Status.NotFound), Some(ErrorAttributes.ErrorType("404")))
    assertEquals(
      OtelHttp4sAttributes.errorType(Status.InternalServerError),
      Some(ErrorAttributes.ErrorType("500"))
    )
  }

  test("statusCode and requestMethod") {
    assertEquals(OtelHttp4sAttributes.statusCode(Status.Created), HttpAttributes.HttpResponseStatusCode(201L))
    assertEquals(
      OtelHttp4sAttributes.requestMethod(Request[IO](method = Method.POST)),
      HttpAttributes.HttpRequestMethod("POST")
    )
  }

  test("serverAddress is derived from the host when present") {
    assertEquals(
      OtelHttp4sAttributes.serverAddress(uri("http://example.com/x").host),
      Some(ServerAttributes.ServerAddress("example.com"))
    )
    assertEquals(OtelHttp4sAttributes.serverAddress(None), None)
  }

  test("serverPort falls back: remote port, then url port, then scheme default, then none") {
    // explicit remote port wins
    assertEquals(
      OtelHttp4sAttributes.serverPort(Some(port(9999)), uri("http://example.com:8080/x")),
      Some(ServerAttributes.ServerPort(9999L))
    )
    // url port when no remote port
    assertEquals(
      OtelHttp4sAttributes.serverPort(None, uri("http://example.com:8080/x")),
      Some(ServerAttributes.ServerPort(8080L))
    )
    // scheme default (http -> 80, https -> 443) when neither is set
    assertEquals(
      OtelHttp4sAttributes.serverPort(None, uri("http://example.com/x")),
      Some(ServerAttributes.ServerPort(80L))
    )
    assertEquals(
      OtelHttp4sAttributes.serverPort(None, uri("https://example.com/x")),
      Some(ServerAttributes.ServerPort(443L))
    )
    // nothing to derive from
    assertEquals(OtelHttp4sAttributes.serverPort(None, uri("/relative")), None)
  }

  test("uriScheme and uriFull") {
    assertEquals(OtelHttp4sAttributes.uriScheme(Some(Uri.Scheme.https)), Some(UrlAttributes.UrlScheme("https")))
    assertEquals(OtelHttp4sAttributes.uriScheme(None), None)
    assertEquals(
      OtelHttp4sAttributes.uriFull(uri("http://example.com/x?q=1")),
      UrlAttributes.UrlFull("http://example.com/x?q=1")
    )
  }

  test("networkPeerAddress and networkPeerPort") {
    val ip = IpAddress.fromString("10.0.0.1").get
    assertEquals(OtelHttp4sAttributes.networkPeerAddress(ip), NetworkAttributes.NetworkPeerAddress("10.0.0.1"))
    assertEquals(OtelHttp4sAttributes.networkPeerPort(port(443)), NetworkAttributes.NetworkPeerPort(443L))
  }

  test("headers: only allow-listed headers are kept, grouped by name, keyed by lowercased name") {
    val headers = Headers(
      Header.Raw(ci"Accept", "application/json"),
      Header.Raw(ci"Accept", "text/plain"),
      Header.Raw(ci"X-Custom-Not-Listed", "secret")
    )
    val result  = OtelHttp4sAttributes
      .headers(headers, HttpAttributes.HttpRequestHeader)
      .map(attr => attr.key.name -> attr.value)
      .toList

    assertEquals(result, List("http.request.header.accept" -> Seq("application/json", "text/plain")))
  }
}
