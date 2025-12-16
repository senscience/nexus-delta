package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticsearchMappings, ElasticsearchSettings}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.IO
import io.circe.JsonObject

package object views {

  opaque type DefaultIndexDef = ElasticsearchIndexDef

  object DefaultIndexDef {
    def load()(using loader: ClasspathResourceLoader): IO[DefaultIndexDef] =
      ElasticsearchIndexDef.load(
        "defaults/default-mapping.json",
        Some("defaults/default-settings.json"),
        "number_of_shards" -> 1
      )

    def fromJson(mapping: JsonObject, settings: JsonObject): DefaultIndexDef =
      ElasticsearchIndexDef.fromJson(mapping, Some(settings))

    extension (defaultIndexDef: DefaultIndexDef) {
      def fallbackUnless(
          mappings: Option[ElasticsearchMappings],
          settings: Option[ElasticsearchSettings]
      ): ElasticsearchIndexDef         =
        ElasticsearchIndexDef(
          mappings.getOrElse(defaultIndexDef.mappings),
          settings.orElse(defaultIndexDef.settings)
        )
      def value: ElasticsearchIndexDef = defaultIndexDef
    }
  }

}
