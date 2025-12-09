package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sourcing.model.Tags
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Json, JsonObject}

final case class MainDocument private (payload: Json) extends AnyVal

object MainDocument {

  def apply(
      name: Option[String],
      label: Option[String],
      prefLabel: Option[String],
      description: Option[String],
      metadata: ResourceF[Unit],
      tags: Tags,
      originalSource: Json,
      additionalFields: JsonObject
  )(using BaseUri) =
    new MainDocument(
      additionalFields
        .deepMerge(metadata.asJsonObject)
        .deepMerge(
          JsonObject(
            "name"             := name,
            "label"            := label,
            "prefLabel"        := prefLabel,
            "description"      := description,
            "_original_source" := originalSource.removeAllKeys(keywords.context).noSpaces,
            "_tags"            := Option.when(tags.value.nonEmpty)(tags.value.keys)
          )
        )
        .asJson
    )

  def unsafe(payload: Json) = new MainDocument(payload)

}
