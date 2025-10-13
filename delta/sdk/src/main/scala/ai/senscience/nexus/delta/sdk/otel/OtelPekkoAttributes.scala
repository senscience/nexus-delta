package ai.senscience.nexus.delta.sdk.otel

import org.apache.pekko.http.scaladsl.model.headers.`X-Forwarded-Proto`
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.semconv.attributes.{ErrorAttributes, HttpAttributes, NetworkAttributes, UrlAttributes}

object OtelPekkoAttributes {

  def errorType(throwable: Throwable): Attribute[String] =
    errorType(throwable.getClass.getName)

  def errorType(value: String): Attribute[String] =
    ErrorAttributes.ErrorType(value)

  def errorTypeFromStatus(response: HttpResponse): Option[Attribute[String]] =
    Option.when(serverError(response))(errorType(response.status.value))

  def networkProtocolName(request: HttpRequest): Option[Attribute[String]] =
    Option.when(request.protocol.value.startsWith("HTTP/"))(NetworkAttributes.NetworkProtocolName("http"))

  def networkProtocolVersion(request: HttpRequest): Option[Attribute[String]] = {
    val protocol = request.protocol.value
    Option.when(protocol.startsWith("HTTP/"))(
      NetworkAttributes.NetworkProtocolVersion(protocol.substring("HTTP/".length))
    )
  }

  def originalScheme(request: HttpRequest): Attribute[String] = {
    val originalScheme = request.header[`X-Forwarded-Proto`].map(_.protocol).getOrElse(request.uri.scheme)
    UrlAttributes.UrlScheme(originalScheme)
  }

  def requestMethod(request: HttpRequest): Attribute[String] =
    HttpAttributes.HttpRequestMethod(request.method.name())

  def responseStatus(response: HttpResponse): Attribute[Long] =
    HttpAttributes.HttpResponseStatusCode(response.status.intValue)

  def urlPath(path: Uri.Path): Option[Attribute[String]] =
    Option.unless(path.isEmpty)(UrlAttributes.UrlPath(path.toString))

  def urlQuery(query: Uri.Query): Option[Attribute[String]] =
    Option.unless(query.isEmpty)(UrlAttributes.UrlQuery(query.toString))

  def serverError(response: HttpResponse): Boolean = response.status.intValue > 500
}
