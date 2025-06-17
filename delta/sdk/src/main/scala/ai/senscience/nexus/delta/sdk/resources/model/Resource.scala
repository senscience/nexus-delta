package ai.senscience.nexus.delta.sdk.resources.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, JsonLdOptions}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.rdf.syntax.jsonOpsSyntax
import ai.senscience.nexus.delta.sdk.ResourceShift
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegmentRef, ResourceF}
import ai.senscience.nexus.delta.sdk.resources.Resources
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef, Tags}
import cats.effect.IO
import io.circe.Json

/**
  * A resource representation
  *
  * @param id
  *   the resource identifier
  * @param project
  *   the project where the resource belongs
  * @param tags
  *   the resource tags
  * @param schema
  *   the schema used to constrain the resource
  * @param source
  *   the representation of the resource as posted by the subject
  * @param compacted
  *   the compacted JSON-LD representation of the resource
  * @param expanded
  *   the expanded JSON-LD representation of the resource
  */
final case class Resource(
    id: Iri,
    project: ProjectRef,
    tags: Tags,
    schema: ResourceRef,
    source: Json,
    compacted: CompactedJsonLd,
    expanded: ExpandedJsonLd
)

object Resource {

  implicit val resourceJsonLdEncoder: JsonLdEncoder[Resource] =
    new JsonLdEncoder[Resource] {

      override def compact(
          value: Resource
      )(implicit opts: JsonLdOptions, api: JsonLdApi, rcr: RemoteContextResolution): IO[CompactedJsonLd] =
        IO.pure(value.compacted)

      override def expand(
          value: Resource
      )(implicit opts: JsonLdOptions, api: JsonLdApi, rcr: RemoteContextResolution): IO[ExpandedJsonLd] =
        IO.pure(value.expanded)

      override def context(value: Resource): ContextValue =
        value.source.topContextValueOrEmpty
    }

  def toJsonLdContent(value: ResourceF[Resource]): JsonLdContent[Resource] =
    JsonLdContent(value, value.value.source, value.value.tags)

  type Shift = ResourceShift[ResourceState, Resource]

  def shift(resources: Resources)(implicit baseUri: BaseUri): Shift =
    ResourceShift[ResourceState, Resource](
      Resources.entityType,
      (ref, project) => resources.fetch(IdSegmentRef(ref), project, None),
      state => state.toResource,
      toJsonLdContent
    )
}
