package ai.senscience.nexus.delta.elasticsearch.client

import io.circe.JsonObject
import io.circe.syntax.EncoderOps

/**
  * Contains utility methods for Elasticsearch responses containing hits
  */
object Hits {

  def fetchTotal(json: JsonObject): Long =
    json.asJson.hcursor.downField("hits").downField("total").get[Long]("value").getOrElse(0L)
}
