package ai.senscience.nexus.delta.sdk.generators

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.SchemaResource
import ai.senscience.nexus.delta.sdk.schemas.model.{Schema, SchemaState}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tags}
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.Json

import java.time.Instant
import scala.concurrent.duration.DurationInt

object SchemaGen {
  // We put a lenient api for schemas otherwise the api checks data types before the actual schema validation process
  implicit val api: JsonLdApi = TitaniumJsonLdApi.lenient

  def empty(id: Iri, project: ProjectRef) =
    SchemaState(
      id,
      project,
      Json.obj(),
      CompactedJsonLd.empty,
      NonEmptyList.of(ExpandedJsonLd.empty),
      1,
      deprecated = false,
      Tags.empty,
      Instant.EPOCH,
      Anonymous,
      Instant.EPOCH,
      Anonymous
    ).toResource

  def currentState(
      schema: Schema,
      rev: Int = 1,
      deprecated: Boolean = false,
      subject: Subject = Anonymous
  ): SchemaState = {
    SchemaState(
      schema.id,
      schema.project,
      schema.source,
      schema.compacted,
      schema.expanded,
      rev,
      deprecated,
      schema.tags,
      Instant.EPOCH,
      subject,
      Instant.EPOCH,
      subject
    )
  }

  def schema(
      id: Iri,
      project: ProjectRef,
      source: Json,
      tags: Tags = Tags.empty
  )(implicit resolution: RemoteContextResolution): Schema = {
    schemaAsync(id, project, source, tags).accepted
  }

  def schemaAsync(
      id: Iri,
      project: ProjectRef,
      source: Json,
      tags: Tags = Tags.empty
  )(implicit resolution: RemoteContextResolution): IO[Schema] = {
    for {
      expanded  <- ExpandedJsonLd(source).map(_.replaceId(id))
      compacted <- expanded.toCompacted(source.topContextValueOrEmpty)
    } yield {
      Schema(id, project, tags, source, compacted, NonEmptyList.of(expanded))
    }
  }

  def resourceFor(
      schema: Schema,
      rev: Int = 1,
      subject: Subject = Anonymous,
      deprecated: Boolean = false
  ): SchemaResource =
    SchemaState(
      schema.id,
      schema.project,
      schema.source,
      schema.compacted,
      schema.expanded,
      rev,
      deprecated,
      schema.tags,
      Instant.EPOCH,
      subject,
      Instant.EPOCH,
      subject
    ).toResource

  implicit final private class CatsIOValuesOps[A](private val io: IO[A]) {
    def accepted: A =
      io.unsafeRunTimed(45.seconds).getOrElse(throw new RuntimeException("IO timed out during .accepted call"))
  }
}
