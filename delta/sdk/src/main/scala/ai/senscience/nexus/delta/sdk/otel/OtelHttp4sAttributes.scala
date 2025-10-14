package ai.senscience.nexus.delta.sdk.otel

import cats.effect.IO
import com.comcast.ip4s.{IpAddress, Port}
import org.http4s.{Headers, Request, Status, Uri}
import org.typelevel.otel4s.semconv.attributes.*
import org.typelevel.otel4s.{Attribute, AttributeKey}

import java.util.Locale
import scala.collection.View

object OtelHttp4sAttributes {

  def errorType(cause: Throwable): Attribute[String] =
    errorType(cause.getClass.getName)

  def errorType(value: String): Attribute[String] =
    ErrorAttributes.ErrorType(value)

  def errorType(status: Status): Option[Attribute[String]] =
    Option.unless(status.isSuccess)(errorType(s"${status.code}"))

  def headers(headers: Headers, headerAttribute: AttributeKey[Seq[String]]): View[Attribute[Seq[String]]] =
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

  def networkPeerAddress(ip: IpAddress): Attribute[String] =
    NetworkAttributes.NetworkPeerAddress(ip.toString)

  def networkPeerPort(port: Port): Attribute[Long] =
    NetworkAttributes.NetworkPeerPort(port.value.toLong)

  def requestMethod(request: Request[IO]): Attribute[String] =
    HttpAttributes.HttpRequestMethod(request.method.name)

  def serverAddress(host: Option[Uri.Host]): Option[Attribute[String]] =
    host.map { h => ServerAttributes.ServerAddress(h.value) }

  def serverPort(
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

  def statusCode(status: Status): Attribute[Long] =
    HttpAttributes.HttpResponseStatusCode(status.code.toLong)

  def uriFull(uri: Uri): Attribute[String] =
    UrlAttributes.UrlFull(uri.renderString)

  def uriScheme(scheme: Option[Uri.Scheme]): Option[Attribute[String]] =
    scheme.map { s => UrlAttributes.UrlScheme(s.value) }

}
