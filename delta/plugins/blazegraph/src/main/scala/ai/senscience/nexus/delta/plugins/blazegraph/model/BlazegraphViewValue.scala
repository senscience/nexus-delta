package ai.senscience.nexus.delta.plugins.blazegraph.model

import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.IndexingBlazegraphViewValue
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.decoder.configuration.semiauto.deriveConfigJsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.{Configuration, JsonLdDecoder}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.IriFilter
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.PipeChain
import cats.data.NonEmptySet
import io.circe.generic.extras
import io.circe.generic.extras.semiauto.{deriveConfiguredCodec, deriveConfiguredEncoder}
import io.circe.syntax.*
import io.circe.{Codec, Encoder, Json}

/**
  * Enumeration of Blazegraph view values.
  */
sealed trait BlazegraphViewValue extends Product with Serializable {

  /**
    * @return
    *   the name of the view
    */
  def name: Option[String]

  /**
    * @return
    *   the description of the view
    */
  def description: Option[String]

  /**
    * @return
    *   the view type
    */
  def tpe: BlazegraphViewType

  def toJson(iri: Iri): Json = this.asJsonObject.add(keywords.id, iri.asJson).asJson.dropNullValues

  def asIndexingValue: Option[IndexingBlazegraphViewValue] =
    this match {
      case v: IndexingBlazegraphViewValue => Some(v)
      case _                              => None
    }
}

object BlazegraphViewValue {

  /**
    * The configuration of the Blazegraph view that indexes resources as triples.
    *
    * @param resourceSchemas
    *   the set of schemas considered that constrains resources; empty implies all
    * @param resourceTypes
    *   the set of resource types considered for indexing; empty implies all
    * @param resourceTag
    *   an optional tag to consider for indexing; when set, all resources that are tagged with the value of the field
    *   are indexed with the corresponding revision
    * @param includeMetadata
    *   whether to include the metadata of the resource as individual fields in the document
    * @param includeDeprecated
    *   whether to consider deprecated resources for indexing
    * @param permission
    *   the permission required for querying this view
    */
  final case class IndexingBlazegraphViewValue(
      name: Option[String] = None,
      description: Option[String] = None,
      resourceSchemas: IriFilter = IriFilter.None,
      resourceTypes: IriFilter = IriFilter.None,
      resourceTag: Option[UserTag] = None,
      includeMetadata: Boolean = false,
      includeDeprecated: Boolean = false,
      permission: Permission = permissions.query
  ) extends BlazegraphViewValue {
    override val tpe: BlazegraphViewType = BlazegraphViewType.IndexingBlazegraphView

    /**
      * Translates the view into a [[PipeChain]]
      */
    def pipeChain: Option[PipeChain] = PipeChain(resourceSchemas, resourceTypes, includeMetadata, includeDeprecated)

    /**
      * Returns true if this [[IndexingBlazegraphViewValue]] is equal to the provided [[IndexingBlazegraphViewValue]] on
      * the fields which should trigger a reindexing of the view when modified.
      */
    def hasSameIndexingFields(that: IndexingBlazegraphViewValue): Boolean =
      resourceSchemas == that.resourceSchemas &&
        resourceTypes == that.resourceTypes &&
        resourceTag == that.resourceTag &&
        includeMetadata == that.includeMetadata &&
        includeDeprecated == that.includeDeprecated

    /**
      * Creates a [[SelectFilter]] for this view
      */
    def selectFilter: SelectFilter = SelectFilter.tagOpt(resourceTag)
  }

  /**
    * @return
    *   the next indexing revision based on the differences between the given views
    */
  def nextIndexingRev(
      view1: IndexingBlazegraphViewValue,
      view2: IndexingBlazegraphViewValue,
      currentRev: Int
  ): Int =
    if (!view1.hasSameIndexingFields(view2)) currentRev + 1
    else currentRev

  /**
    * The configuration of the Blazegraph view that delegates queries to multiple namespaces.
    *
    * @param views
    *   the collection of views where queries will be delegated (if necessary permissions are met)
    */
  final case class AggregateBlazegraphViewValue(
      name: Option[String],
      description: Option[String],
      views: NonEmptySet[ViewRef]
  ) extends BlazegraphViewValue {
    override val tpe: BlazegraphViewType = BlazegraphViewType.AggregateBlazegraphView
  }

  implicit private val blazegraphViewValueEncoder: Encoder.AsObject[BlazegraphViewValue] = {
    import io.circe.generic.extras.Configuration

    implicit val config: Configuration = Configuration(
      transformMemberNames = identity,
      transformConstructorNames = {
        case "IndexingBlazegraphViewValue"  => BlazegraphViewType.IndexingBlazegraphView.toString
        case "AggregateBlazegraphViewValue" => BlazegraphViewType.AggregateBlazegraphView.toString
        case other                          => other
      },
      useDefaults = false,
      discriminator = Some(keywords.tpe),
      strictDecoding = false
    )
    deriveConfiguredEncoder[BlazegraphViewValue]
  }

  implicit def blazegraphViewValueJsonLdDecoder(implicit
      configuration: Configuration
  ): JsonLdDecoder[BlazegraphViewValue] =
    deriveConfigJsonLdDecoder[BlazegraphViewValue]

  object Database {
    implicit private val configuration: extras.Configuration    = Serializer.circeConfiguration
    implicit val bgvvCodec: Codec.AsObject[BlazegraphViewValue] = deriveConfiguredCodec[BlazegraphViewValue]
  }
}
