package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import cats.effect.IO
import io.circe.syntax.EncoderOps
import io.circe.*

import scala.annotation.targetName

package object client {

  type CreateIndex = (IndexLabel, ElasticsearchIndexDef) => IO[Unit]

  opaque type ElasticsearchMappings = JsonObject

  object ElasticsearchMappings {
    def apply(jsonObject: JsonObject): ElasticsearchMappings = jsonObject

    given Codec[ElasticsearchMappings] =
      Codec.from(
        Decoder.decodeJsonObject.map(ElasticsearchMappings(_)),
        Encoder.AsObject.instance(identity)
      )

    given JsonLdDecoder[ElasticsearchMappings] = JsonLdDecoder.jsonObjectJsonLdDecoder.map(ElasticsearchMappings(_))
  }

  extension (mapping: ElasticsearchMappings) {
    def value: Json = mapping.asJson
  }

  opaque type ElasticsearchSettings = JsonObject

  object ElasticsearchSettings {
    def apply(jsonObject: JsonObject): ElasticsearchSettings = jsonObject

    given Codec[ElasticsearchSettings] =
      Codec.from(
        Decoder.decodeJsonObject.map(ElasticsearchSettings(_)),
        Encoder.AsObject.instance(identity)
      )

    given JsonLdDecoder[ElasticsearchSettings] = JsonLdDecoder.jsonObjectJsonLdDecoder.map(ElasticsearchSettings(_))
  }

  extension (settings: ElasticsearchSettings) {
    @targetName("settings_value")
    def value: Json = settings.asJson
  }

}
