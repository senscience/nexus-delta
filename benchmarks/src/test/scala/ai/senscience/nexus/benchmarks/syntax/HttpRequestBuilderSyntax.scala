package ai.senscience.nexus.benchmarks.syntax

import ai.senscience.nexus.benchmarks.syntax.HttpRequestBuilderSyntax.defaultHeaders
import io.circe.Json
import io.gatling.core.Predef.*
import io.gatling.http.request.builder.HttpRequestBuilder

trait HttpRequestBuilderSyntax {

  implicit final def httpProtocolBuilderSyntax(http: HttpRequestBuilder): HttpRequestBuilderOps =
    new HttpRequestBuilderOps(http)

}

final class HttpRequestBuilderOps(val http: HttpRequestBuilder) extends AnyVal {

  def jsonBody(json: Json): HttpRequestBuilder =
    http
      .headers(defaultHeaders)
      .body(StringBody(json.noSpaces))
}

object HttpRequestBuilderSyntax {
  val defaultHeaders = Map(
    "Content-Type" -> "application/json"
  )
}
