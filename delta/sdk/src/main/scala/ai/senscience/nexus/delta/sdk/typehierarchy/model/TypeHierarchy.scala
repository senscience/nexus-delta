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

  private given Configuration = Configuration.default

  given Decoder[TypeHierarchy] = deriveConfiguredDecoder[TypeHierarchy]

  given Encoder.AsObject[TypeHierarchy] = deriveConfiguredEncoder[TypeHierarchy]

  private val context: ContextValue = ContextValue(contexts.typeHierarchy)

  given JsonLdEncoder[TypeHierarchy] = JsonLdEncoder.computeFromCirce(context)
}
