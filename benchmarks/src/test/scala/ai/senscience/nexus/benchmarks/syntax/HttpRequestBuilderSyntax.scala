package ai.senscience.nexus.benchmarks.syntax

import ai.senscience.nexus.benchmarks.syntax.HttpRequestBuilderSyntax.defaultHeaders
import io.circe.Json
import io.gatling.core.Predef.*
import io.gatling.http.request.builder.HttpRequestBuilder

trait HttpRequestBuilderSyntax {

  extension (http: HttpRequestBuilder) {
    def jsonBody(json: Json): HttpRequestBuilder =
      http
        .headers(defaultHeaders)
        .body(StringBody(json.noSpaces))
  }
}

object HttpRequestBuilderSyntax {
  val defaultHeaders: Map[String, String] = Map(
    "Content-Type" -> "application/json"
  )
}
