package ai.senscience.nexus.delta.sdk.resources.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.RdfError
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.DataResource
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdAssembly
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.InvalidJsonLdFormat
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef, Tags}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import cats.effect.IO
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, Json}

import java.time.Instant

/**
  * A resource active state.
  *
  * @param id
  *   the resource identifier
  * @param project
  *   the project where the resource belongs
  * @param schemaProject
  *   the project where the schema belongs
  * @param source
  *   the representation of the resource as posted by the subject
  * @param compacted
  *   the compacted JSON-LD representation of the resource
  * @param expanded
  *   the expanded JSON-LD representation of the resource
  * @param remoteContexts
  *   the remote contexts of the resource
  * @param rev
  *   the organization revision
  * @param deprecated
  *   the deprecation status of the organization
  * @param schema
  *   the optional schema used to constrain the resource
  * @param types
  *   the collection of known resource types
  * @param tags
  *   the collection of tag aliases
  * @param createdAt
  *   the instant when the resource was created
  * @param createdBy
  *   the identity that created the resource
  * @param updatedAt
  *   the instant when the resource was last updated
  * @param updatedBy
  *   the identity that last updated the resource
  */
final case class ResourceState(
    id: Iri,
    project: ProjectRef,
    schemaProject: ProjectRef,
    source: Json,
    compacted: CompactedJsonLd,
    expanded: ExpandedJsonLd,
    // TODO: Remove default after 1.10 migration
    remoteContexts: Set[RemoteContextRef] = Set.empty,
    rev: Int,
    deprecated: Boolean,
    schema: ResourceRef,
    types: Set[Iri],
    tags: Tags,
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends ScopedState {

  def toAssembly: IO[JsonLdAssembly] = {
    given JsonLdApi = TitaniumJsonLdApi.lenient
    expanded.toGraph
      .map { graph =>
        JsonLdAssembly(id, source, compacted, expanded, graph, remoteContexts)
      }
      .adaptError { case err: RdfError => InvalidJsonLdFormat(Some(id), err) }
  }

  def toResource: DataResource =
    ResourceF(
      id = id,
      access = ResourceAccess.resource(project, id),
      rev = rev,
      types = types,
      schema = schema,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      value = Resource(id, project, tags, schema, source, compacted, expanded)
    )
}

object ResourceState {

  given serializer: Serializer[Iri, ResourceState] = {
    import ai.senscience.nexus.delta.rdf.jsonld.CompactedJsonLd.Database.given
    import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd.Database.given
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.given

    // TODO: The `.withDefaults` method is used in order to inject the default empty remoteContexts
    //  when deserializing an event that has none. Remove it after 1.10 migration.
    given Configuration                 = Serializer.circeConfiguration.withDefaults
    given Codec.AsObject[ResourceState] = deriveConfiguredCodec[ResourceState]
    Serializer()
  }

}
