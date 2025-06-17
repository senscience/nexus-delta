package ai.senscience.nexus.delta.sdk.typehierarchy.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchy.TypeHierarchyMapping
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}

/**
  * A type hierarchy representation.
  * @param mapping
  *   type hierarchy mapping
  */
case class TypeHierarchy(mapping: TypeHierarchyMapping)

object TypeHierarchy {
  type TypeHierarchyMapping = Map[Iri, Set[Iri]]

  implicit private val config: Configuration = Configuration.default

  implicit val typeHierarchyMappingDecoder: Decoder[TypeHierarchy] =
    deriveConfiguredDecoder[TypeHierarchy]

  implicit val typeHierarchyEncoder: Encoder.AsObject[TypeHierarchy] =
    deriveConfiguredEncoder[TypeHierarchy]

  val context: ContextValue = ContextValue(contexts.typeHierarchy)

  implicit val typeHierarchyJsonLdEncoder: JsonLdEncoder[TypeHierarchy] =
    JsonLdEncoder.computeFromCirce(context)
}
