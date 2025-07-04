package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeView.{Metadata, RebuildStrategy}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, JsonLdOptions}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tags}
import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.IO
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder
import io.circe.parser.parse
import io.circe.syntax.*
import io.circe.{Encoder, Json, JsonObject}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/**
  * Representation of a composite view.
  *
  * @param id
  *   the id of the view
  * @param project
  *   the project to which this view belongs
  * @param sources
  *   the collection of sources for the view
  * @param projections
  *   the collection of projections for the view
  * @param rebuildStrategy
  *   the rebuild strategy of the view
  * @param uuid
  *   the uuid of the view
  * @param tags
  *   the tag -> rev mapping
  * @param source
  *   the original json document provided at creation or update
  * @param updatedAt
  *   the instant when the view was last updated
  */
final case class CompositeView(
    id: Iri,
    project: ProjectRef,
    name: Option[String],
    description: Option[String],
    sources: NonEmptyMap[Iri, CompositeViewSource],
    projections: NonEmptyMap[Iri, CompositeViewProjection],
    rebuildStrategy: Option[RebuildStrategy],
    uuid: UUID,
    tags: Tags,
    source: Json,
    updatedAt: Instant
) {

  /**
    * @return
    *   [[CompositeView]] metadata
    */
  def metadata: Metadata = Metadata(uuid)
}

object CompositeView {

  def apply(
      id: Iri,
      project: ProjectRef,
      sources: NonEmptyList[CompositeViewSource],
      projections: NonEmptyList[CompositeViewProjection],
      rebuildStrategy: Option[RebuildStrategy],
      uuid: UUID,
      tags: Tags,
      source: Json,
      updatedAt: Instant
  ): CompositeView = CompositeView(
    id,
    project,
    None,
    None,
    sources.map { s => s.id -> s }.toNem,
    projections.map { p => p.id -> p }.toNem,
    rebuildStrategy,
    uuid,
    tags,
    source,
    updatedAt
  )

  /**
    * The rebuild strategy for a [[CompositeView]].
    */
  sealed trait RebuildStrategy extends Product with Serializable

  /**
    * Rebuild strategy defining rebuilding at a certain interval.
    */
  final case class Interval(value: FiniteDuration) extends RebuildStrategy

  final case class Metadata(uuid: UUID)

  object RebuildStrategy {

    implicit final val rebuildStrategyEncoder: Encoder.AsObject[RebuildStrategy] = {
      implicit val config: Configuration                          = Configuration.default.withDiscriminator(keywords.tpe)
      implicit val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder.encodeString.contramap(_.toString())
      deriveConfiguredEncoder[RebuildStrategy]
    }
  }

  implicit private def compositeViewEncoder(implicit base: BaseUri): Encoder.AsObject[CompositeView] = {
    implicit val config: Configuration = Configuration.default.withDiscriminator(keywords.tpe)
    import ai.senscience.nexus.delta.sdk.circe.nonEmptyMap.*
    Encoder.encodeJsonObject.contramapObject { v =>
      deriveConfiguredEncoder[CompositeView]
        .encodeObject(v)
        .add(keywords.tpe, Set(nxv + "View", compositeViewType).asJson)
        .remove("tags")
        .remove("project")
        .remove("source")
        .remove("id")
        .remove("updatedAt")
        .mapAllKeys("context", _.noSpaces.asJson)
        .mapAllKeys("mapping", _.noSpaces.asJson)
        .mapAllKeys("settings", _.noSpaces.asJson)
        .removeAllKeys("indexingRev")
        .addContext(v.source.topContextValueOrEmpty.excludeRemoteContexts.contextObj)
    }
  }

  implicit def compositeViewJsonLdEncoder(implicit base: BaseUri): JsonLdEncoder[CompositeView] = {
    val underlying: JsonLdEncoder[CompositeView] =
      JsonLdEncoder.computeFromCirce(_.id, ContextValue(contexts.compositeViews))
    new JsonLdEncoder[CompositeView] {

      private def parseJson(jsonString: Json) = jsonString.asString.fold(jsonString)(parse(_).getOrElse(jsonString))

      private def stringToJson(obj: JsonObject) =
        obj.mapAllKeys("mapping", parseJson).mapAllKeys("settings", parseJson).mapAllKeys("context", parseJson)

      override def context(value: CompositeView): ContextValue = underlying.context(value)

      override def expand(
          value: CompositeView
      )(implicit opts: JsonLdOptions, api: JsonLdApi, rcr: RemoteContextResolution): IO[ExpandedJsonLd] =
        underlying.expand(value)

      override def compact(
          value: CompositeView
      )(implicit opts: JsonLdOptions, api: JsonLdApi, rcr: RemoteContextResolution): IO[CompactedJsonLd] =
        underlying.compact(value).map { c =>
          CompactedJsonLd.unsafe(c.rootId, c.ctx, stringToJson(c.obj))
        }
    }
  }

  implicit private val compositeViewMetadataEncoder: Encoder.AsObject[Metadata] =
    Encoder.encodeJsonObject.contramapObject(meta => JsonObject("_uuid" -> meta.uuid.asJson))

  implicit val compositeViewMetadataJsonLdEncoder: JsonLdEncoder[Metadata] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.compositeViewsMetadata))
}
