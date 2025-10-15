package ai.senscience.nexus.delta.elasticsearch.main

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.IO
import io.circe.JsonObject

/**
  * Configuration for the main index
  * @param name
  *   name of the index
  * @param mapping
  *   mapping to apply
  * @param settings
  *   settings to apply
  */
final case class MainIndexDef(name: IndexLabel, mapping: JsonObject, settings: JsonObject)

object MainIndexDef {

  def apply(config: MainIndexConfig)(using loader: ClasspathResourceLoader): IO[MainIndexDef] =
    for {
      dm <- loader.jsonObjectContentOf("defaults/default-mapping.json")
      ds <- loader.jsonObjectContentOf("defaults/default-settings.json", "number_of_shards" -> config.shards)
    } yield MainIndexDef(config.index, dm, ds)

}
