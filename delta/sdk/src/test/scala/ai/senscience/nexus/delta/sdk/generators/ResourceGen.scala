package ai.senscience.nexus.delta.sdk.generators

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.schemas
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.DataResource
import ai.senscience.nexus.delta.sdk.jsonld.{JsonLdAssembly, JsonLdContent}
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.resources.model.{Resource, ResourceState}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef, Tags}
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.Json

import java.time.Instant
import scala.concurrent.duration.DurationInt

object ResourceGen {

  private given JsonLdApi = TitaniumJsonLdApi.strict

  def currentState(
      project: ProjectRef,
      jsonld: JsonLdAssembly,
      schema: ResourceRef = Latest(schemas.resources),
      tags: Tags = Tags.empty,
      rev: Int = 1,
      deprecated: Boolean = false,
      subject: Subject = Anonymous
  )                                          =
    ResourceState(
      jsonld.id,
      project,
      project,
      jsonld.source,
      jsonld.compacted,
      jsonld.expanded,
      jsonld.remoteContexts,
      rev,
      deprecated,
      schema,
      jsonld.types,
      tags,
      Instant.EPOCH,
      subject,
      Instant.EPOCH,
      subject
    )
  def resource(
      id: Iri,
      project: ProjectRef,
      source: Json,
      schema: ResourceRef = Latest(schemas.resources),
      tags: Tags = Tags.empty
  )(using RemoteContextResolution): Resource = {
    val expanded  = ExpandedJsonLd(source).accepted.replaceId(id)
    val compacted = expanded.toCompacted(source.topContextValueOrEmpty).accepted
    Resource(id, project, tags, schema, source, compacted, expanded)
  }

  def sourceToResourceF(
      id: Iri,
      project: ProjectRef,
      source: Json,
      schema: ResourceRef = Latest(schemas.resources),
      tags: Tags = Tags.empty,
      rev: Int = 1,
      subject: Subject = Anonymous,
      deprecated: Boolean = false
  )(using RemoteContextResolution): DataResource = {
    val result         = ExpandedJsonLd.explain(source).accepted
    val expanded       = result.value.replaceId(id)
    val compacted      = expanded.toCompacted(source.topContextValueOrEmpty).accepted
    val remoteContexts = RemoteContextRef(result.remoteContexts)
    Resource(id, project, tags, schema, source, compacted, expanded)
    ResourceState(
      id,
      project,
      project,
      source,
      compacted,
      expanded,
      remoteContexts,
      rev,
      deprecated,
      schema,
      expanded.cursor.getTypes.getOrElse(Set.empty),
      tags,
      Instant.EPOCH,
      subject,
      Instant.EPOCH,
      subject
    ).toResource
  }

  def resourceFor(
      resource: Resource,
      types: Set[Iri] = Set.empty,
      rev: Int = 1,
      subject: Subject = Anonymous,
      deprecated: Boolean = false
  ): DataResource =
    ResourceState(
      resource.id,
      resource.project,
      resource.project,
      resource.source,
      resource.compacted,
      resource.expanded,
      Set.empty,
      rev,
      deprecated,
      resource.schema,
      types,
      resource.tags,
      Instant.EPOCH,
      subject,
      Instant.EPOCH,
      subject
    ).toResource

  def resourceFUnit(id: Iri, project: ProjectRef, types: Set[Iri]): ResourceF[Unit] =
    ResourceF[Unit](
      id = id,
      access = ResourceAccess.resource(project, id),
      rev = 1,
      types = types,
      deprecated = false,
      createdAt = Instant.EPOCH,
      createdBy = Anonymous,
      updatedAt = Instant.EPOCH,
      updatedBy = Anonymous,
      schema = ResourceRef(Vocabulary.schemas.resources),
      value = ()
    )

  def jsonLdContent(id: Iri, project: ProjectRef, source: Json)(using
      RemoteContextResolution
  ): JsonLdContent[Resource] = {
    val resourceF = sourceToResourceF(id, project, source)
    JsonLdContent(resourceF, resourceF.value.source, Tags.empty)
  }

  implicit final private class CatsIOValuesOps[A](private val io: IO[A]) {
    def accepted: A =
      io.unsafeRunTimed(45.seconds).getOrElse(throw new RuntimeException("IO timed out during .accepted call"))
  }

}
