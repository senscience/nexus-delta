package ai.senscience.nexus.delta.sdk.typehierarchy.model

import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sdk.TypeHierarchyResource
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.typehierarchy.TypeHierarchy.typeHierarchyId
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchy.TypeHierarchyMapping
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.state.State.GlobalState
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant

/**
  * Enumeration of type hierarchy states.
  */
final case class TypeHierarchyState(
    mapping: TypeHierarchyMapping,
    rev: Int,
    deprecated: Boolean,
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends GlobalState {

  override def id: Iri                    = typeHierarchyId
  override def schema: ResourceRef        = Latest(schemas.typeHierarchy)
  override def types: Set[IriOrBNode.Iri] = Set(nxv.TypeHierarchy)

  def toResource: TypeHierarchyResource =
    ResourceF(
      id = id,
      access = ResourceAccess.typeHierarchy,
      rev = rev,
      types = types,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      schema = schema,
      value = TypeHierarchy(mapping)
    )
}

object TypeHierarchyState {

  val serializer: Serializer[Iri, TypeHierarchyState] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.given
    given Configuration                      = Serializer.circeConfiguration
    given Codec.AsObject[TypeHierarchyState] = deriveConfiguredCodec[TypeHierarchyState]
    Serializer(identity)
  }

}
