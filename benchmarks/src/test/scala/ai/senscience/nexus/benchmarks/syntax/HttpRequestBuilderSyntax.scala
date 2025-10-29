package ai.senscience.nexus.benchmarks.syntax

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
      .headers(Map("Content-Type" -> "application/json"))
      .body(StringBody(json.noSpaces))

}
