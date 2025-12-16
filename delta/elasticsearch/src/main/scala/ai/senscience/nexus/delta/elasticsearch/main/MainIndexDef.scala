package ai.senscience.nexus.delta.elasticsearch.main

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.IO

/**
  * Definition of the main index
  */
final case class MainIndexDef(name: IndexLabel, indexDef: ElasticsearchIndexDef)

object MainIndexDef {

  def apply(config: MainIndexConfig)(using loader: ClasspathResourceLoader): IO[MainIndexDef] =
    ElasticsearchIndexDef
      .load(
        "defaults/default-mapping.json",
        Some("defaults/default-settings.json"),
        "number_of_shards" -> config.shards
      )
      .map(MainIndexDef(config.index, _))

}
