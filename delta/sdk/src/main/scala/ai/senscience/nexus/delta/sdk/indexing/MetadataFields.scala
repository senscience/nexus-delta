package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.{Json, JsonObject}

/**
  * Layout of Nexus system metadata within Elasticsearch documents.
  *
  * To avoid collisions with metadata fields reserved by Elasticsearch Serverless at the root level (e.g. `_project`),
  * all `_`-prefixed system metadata is nested under a single umbrella field.
  */
object MetadataFields {

  /** Umbrella field under which all `_`-prefixed system metadata is nested. */
  val umbrella: String = "_nexus"

  /** Predicate linking a resource to its nested metadata node; compacts to [[umbrella]]. */
  val iri: Iri = nxv + "nexus"

  /**
    * Rewrites a logical field name to its physical path in the index: `_`-prefixed system metadata is nested under
    * [[umbrella]], while data fields (`@id`, `@type`, `name`, `_keywords`, ...) stay at the root.
    */
  def indexField(field: String): String =
    if field.startsWith("_") && !field.startsWith("_keywords") then s"$umbrella.$field" else field

  /**
    * Nests every `_`-prefixed top-level field of [[json]] under [[umbrella]], leaving the data fields (`@id`, `@type`,
    * ...) at the root. Returns [[json]] unchanged when it is not an object or carries no metadata.
    */
  def nest(json: Json): Json =
    json.asObject.fold(json) { obj =>
      val (metadata, data) = obj.toList.partition { case (key, _) => key.startsWith("_") }
      if metadata.isEmpty then json
      else
        Json.fromJsonObject(
          JsonObject.fromIterable(data).add(umbrella, Json.fromJsonObject(JsonObject.fromIterable(metadata)))
        )
    }
}
