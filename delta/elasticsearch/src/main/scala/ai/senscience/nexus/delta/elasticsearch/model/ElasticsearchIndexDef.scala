package ai.senscience.nexus.delta.elasticsearch.model

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticsearchMappings, ElasticsearchSettings, *}
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.IO
import cats.syntax.all.*
import io.circe.{Encoder, JsonObject}

final case class ElasticsearchIndexDef(mappings: ElasticsearchMappings, settings: Option[ElasticsearchSettings])

object ElasticsearchIndexDef {

  given Encoder.AsObject[ElasticsearchIndexDef] = Encoder.AsObject.instance { definition =>
    JsonObject.fromIterable(
      Some("mappings" -> definition.mappings.value) ++
        definition.settings.map { s => "settings" -> s.value }
    )
  }

  def empty: ElasticsearchIndexDef = ElasticsearchIndexDef.fromJson(JsonObject.empty, Some(JsonObject.empty))

  def fromJson(mapping: JsonObject, settings: Option[JsonObject]): ElasticsearchIndexDef =
    ElasticsearchIndexDef(
      ElasticsearchMappings(mapping),
      settings.map(ElasticsearchSettings(_))
    )

  def load(mappingsPath: String, settingsPath: Option[String], settingsAttributes: (String, Any)*)(using
      loader: ClasspathResourceLoader
  ): IO[ElasticsearchIndexDef] =
    for {
      dm <- loader.jsonObjectContentOf(mappingsPath)
      ds <- settingsPath.traverse { loader.jsonObjectContentOf(_, settingsAttributes*) }
    } yield ElasticsearchIndexDef.fromJson(dm, ds)

}
