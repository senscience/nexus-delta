package ai.senscience.nexus.delta.elasticsearch.views

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.IO
import io.circe.JsonObject

/**
  * Default configuration for indices
  * @param mapping
  *   mapping to apply
  * @param settings
  *   settings to apply
  */
final case class DefaultIndexDef(mapping: JsonObject, settings: JsonObject)

object DefaultIndexDef {

  def apply()(using loader: ClasspathResourceLoader): IO[DefaultIndexDef] =
    for {
      dm <- loader.jsonObjectContentOf("defaults/default-mapping.json")
      ds <- loader.jsonObjectContentOf("defaults/default-settings.json", "number_of_shards" -> 1)
    } yield DefaultIndexDef(dm, ds)

}
