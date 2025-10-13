package ai.senscience.nexus.delta.sdk.otel

import cats.effect.IO
import com.comcast.ip4s.{IpAddress, Port}
import org.http4s.{Headers, Request, Response, Status, Uri}
import org.typelevel.otel4s.semconv.attributes.{ErrorAttributes, HttpAttributes, NetworkAttributes, ServerAttributes, UrlAttributes}
import org.typelevel.otel4s.{Attribute, AttributeKey, Attributes}

import java.util.Locale

object OtelHttp4sAttributes {

  def requestAttributes(request: Request[IO]): Attributes = {
    val uri        = request.uri
    val attributes = Attributes.newBuilder
    attributes += requestMethod(request)
    attributes ++= serverAddress(uri.host)
    attributes ++= serverPort(request.remotePort, uri)
    attributes += uriFull(uri)
    attributes ++= uriScheme(uri.scheme)
    request.remote.foreach { socketAddress =>
      attributes += networkPeerAddress(socketAddress.host)
      attributes += networkPeerPort(socketAddress.port)
    }
    attributes ++= headers(request.headers, HttpAttributes.HttpRequestHeader)
    attributes.result()
  }

  def responseAttributes(response: Response[IO]): Attributes = {
    val attributes = Attributes.newBuilder
    attributes += statusCode(response.status)
    attributes ++= Option.unless(response.status.isSuccess)(errorType(response.status))
    attributes ++= headers(response.headers, HttpAttributes.HttpResponseHeader)
    attributes.result()
  }

  def errorType(cause: Throwable): Attribute[String] =
    ErrorAttributes.ErrorType(cause.getClass.getName)

  private def errorType(status: Status): Attribute[String] =
    ErrorAttributes.ErrorType(s"${status.code}")

  private def headers(headers: Headers, headerAttribute: AttributeKey[Seq[String]]) =
    headers
      .redactSensitive()
      .headers
      .groupMap(_.name)(_.value)
      .view
      .flatMap { case (name, values) =>
        Option.when(OtelHeaders.defaultCI.contains(name)) {
          val key = headerAttribute.transformName(_ + "." + name.toString.toLowerCase(Locale.ROOT))
          Attribute(key, values)
        }
      }

  private def networkPeerAddress(ip: IpAddress): Attribute[String] =
    NetworkAttributes.NetworkPeerAddress(ip.toString)

  private def networkPeerPort(port: Port): Attribute[Long] =
    NetworkAttributes.NetworkPeerPort(port.value.toLong)

  private def requestMethod(request: Request[IO]): Attribute[String] =
    HttpAttributes.HttpRequestMethod(request.method.name)

  private def serverAddress(host: Option[Uri.Host]): Option[Attribute[String]] =
    host.map { h => ServerAttributes.ServerAddress(h.value) }

  private def serverPort(
      remotePort: Option[Port],
      url: Uri
  ): Option[Attribute[Long]] =
    remotePort
      .map(_.value.toLong)
      .orElse(url.port.map(_.toLong))
      .orElse(portFromScheme(url.scheme))
      .map(ServerAttributes.ServerPort(_))

  private def portFromScheme(scheme: Option[Uri.Scheme]): Option[Long] =
    scheme.map(_.value.toLowerCase).collect {
      case "http"  => 80L
      case "https" => 443L
    }

  private def statusCode(status: Status) =
    HttpAttributes.HttpResponseStatusCode(status.code.toLong)

  private def uriFull(uri: Uri): Attribute[String] =
    UrlAttributes.UrlFull(uri.renderString)

  private def uriScheme(scheme: Option[Uri.Scheme]) =
    scheme.map { s => UrlAttributes.UrlScheme(s.value) }

}
