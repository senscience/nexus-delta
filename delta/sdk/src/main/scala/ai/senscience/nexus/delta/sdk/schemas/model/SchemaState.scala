package ai.senscience.nexus.delta.sdk.schemas.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.SchemaResource
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef, Tags}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import cats.data.NonEmptyList
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, Json}

import java.time.Instant

/**
  * A schema state.
  *
  * @param id
  *   the schema identifier
  * @param project
  *   the project where the schema belongs
  * @param source
  *   the representation of the schema as posted by the subject
  * @param compacted
  *   the compacted JSON-LD representation of the schema
  * @param expanded
  *   the expanded JSON-LD representation of the schema with the imports resolutions applied
  * @param rev
  *   the organization revision
  * @param deprecated
  *   the deprecation status of the organization
  * @param tags
  *   the collection of tag aliases
  * @param createdAt
  *   the instant when the organization was created
  * @param createdBy
  *   the identity that created the organization
  * @param updatedAt
  *   the instant when the organization was last updated
  * @param updatedBy
  *   the identity that last updated the organization
  */
final case class SchemaState(
    id: Iri,
    project: ProjectRef,
    source: Json,
    compacted: CompactedJsonLd,
    expanded: NonEmptyList[ExpandedJsonLd],
    rev: Int,
    deprecated: Boolean,
    tags: Tags,
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends ScopedState {

  /**
    * @return
    *   the schema reference that schemas conforms to
    */
  def schema: ResourceRef = Latest(schemas.shacl)

  /**
    * @return
    *   the collection of known types of schema resources
    */
  def types: Set[Iri] = Set(nxv.Schema)

  def toResource: SchemaResource =
    ResourceF(
      id = id,
      access = ResourceAccess.schema(project, id),
      rev = rev,
      types = types,
      schema = schema,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      value = Schema(id, project, tags, source, compacted, expanded)
    )
}

object SchemaState {

  implicit val serializer: Serializer[Iri, SchemaState] = {
    import ai.senscience.nexus.delta.rdf.jsonld.CompactedJsonLd.Database.*
    import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd.Database.*
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration       = Serializer.circeConfiguration
    implicit val codec: Codec.AsObject[SchemaState] = deriveConfiguredCodec[SchemaState]
    Serializer.dropNullsInjectType()
  }

}
