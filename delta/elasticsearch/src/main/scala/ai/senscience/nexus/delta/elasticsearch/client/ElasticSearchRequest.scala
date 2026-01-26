package ai.senscience.nexus.delta.elasticsearch.client

import io.circe.{Json, JsonObject}

final case class ElasticSearchRequest(body: JsonObject, queryParams: Map[String, String]) {

  def mapBody(f: JsonObject => JsonObject): ElasticSearchRequest = copy(body = f(body))

  def mapQueryParams(f: Map[String, String] => Map[String, String]): ElasticSearchRequest =
    copy(queryParams = f(queryParams))
}

object ElasticSearchRequest {

  def apply(body: JsonObject): ElasticSearchRequest = new ElasticSearchRequest(body, Map.empty)

  def apply(fields: (String, Json)*): ElasticSearchRequest = ElasticSearchRequest(JsonObject(fields*))
}
