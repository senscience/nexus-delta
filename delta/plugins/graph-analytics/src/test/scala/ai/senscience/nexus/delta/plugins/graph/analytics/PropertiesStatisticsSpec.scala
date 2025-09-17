package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.plugins.graph.analytics.model.PropertiesStatistics
import ai.senscience.nexus.delta.plugins.graph.analytics.model.PropertiesStatistics.propertiesDecoderFromEsAggregations
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import io.circe.Decoder

class PropertiesStatisticsSpec extends CatsEffectSpec with ContextFixtures {

  "PropertiesStatistics" should {

    implicit val jsonLdApi: JsonLdApi = TitaniumJsonLdApi.lenient

    val responseJson = jsonContentOf("paths-properties-aggregations-response.json")
    val expected     = jsonContentOf("properties-tree.json")

    "be converted from Elasticsearch response to client response" in {
      implicit val d: Decoder[PropertiesStatistics] =
        propertiesDecoderFromEsAggregations(iri"https://neuroshapes.org/Trace")
      responseJson.as[PropertiesStatistics].rightValue.toCompactedJsonLd.accepted.json shouldEqual expected
    }
  }

}
