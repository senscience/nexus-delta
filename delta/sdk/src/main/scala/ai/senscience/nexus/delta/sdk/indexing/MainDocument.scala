package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext
import ai.senscience.nexus.delta.sdk.model.ResourceF.ResourceMetadata
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sourcing.model.{Label, Tags}
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Json, JsonObject}

final case class MainDocument private (payload: Json) extends AnyVal

object MainDocument {

  def apply(
      name: Option[String],
      label: Option[String],
      prefLabel: Option[String],
      description: Option[String],
      keywords: Map[Label, String],
      metadata: ResourceF[Unit],
      tags: Tags,
      originalSource: Json,
      additionalMetadata: JsonObject
  )(using BaseUri): MainDocument = {
    val keywordsJson = Option.when(keywords.nonEmpty) {
      val k = keywords.map { case (k, v) => k.value := v }
      Json.obj(k.toSeq*)
    }

    val nexusMetadata = JsonObject(
      "_original_source" := originalSource.removeAllKeys(JsonLdContext.keywords.context).noSpaces,
      "_tags"            := Option.when(tags.value.nonEmpty)(tags.value.keys)
    ).deepMerge(ResourceMetadata(metadata).asJsonObject).deepMerge(additionalMetadata)

    val payload = JsonObject(
      JsonLdContext.keywords.id := metadata.resolvedId,
      "name"                    := name,
      "label"                   := label,
      "prefLabel"               := prefLabel,
      "description"             := description,
      "_keywords"               := keywordsJson,
      MetadataFields.umbrella   := nexusMetadata
    ).addIfNonEmpty(JsonLdContext.keywords.tpe, metadata.types)
    new MainDocument(payload.asJson)
  }

  def unsafe(payload: Json) = new MainDocument(payload)

}
