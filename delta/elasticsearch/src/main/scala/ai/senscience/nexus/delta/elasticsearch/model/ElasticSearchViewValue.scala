package ai.senscience.nexus.delta.elasticsearch.model

import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.IndexingElasticSearchViewValue
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.IndexingElasticSearchViewValue.defaultPipeline
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, PipeStep, ViewRef}
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.IriFilter
import ai.senscience.nexus.delta.sourcing.model.Tag.{Latest, UserTag}
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterByType.FilterByTypeConfig
import ai.senscience.nexus.delta.sourcing.stream.pipes.{DefaultLabelPredicates, DiscardMetadata, FilterByType, FilterDeprecated}
import ai.senscience.nexus.delta.sourcing.stream.{PipeChain, PipeRef}
import cats.data.{NonEmptyChain, NonEmptySet}
import cats.syntax.all.*
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.syntax.*
import io.circe.{Codec, Encoder, Json, JsonObject}

/**
  * Enumeration of ElasticSearch values.
  */
sealed trait ElasticSearchViewValue extends Product with Serializable {

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
  def tpe: ElasticSearchViewType

  def toJson(iri: Iri): Json = {
    import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.Source.*
    this.asJsonObject.add(keywords.id, iri.asJson).asJson.deepDropNullValues
  }

  def asIndexingValue: Option[IndexingElasticSearchViewValue] = this match {
    case v: IndexingElasticSearchViewValue => Some(v)
    case _                                 => None
  }
}

object ElasticSearchViewValue {

  /**
    * The configuration of the ElasticSearch view that indexes resources as documents.
    *
    * @param resourceTag
    *   an optional tag to consider for indexing; when set, all resources that are tagged with the value of the field
    *   are indexed with the corresponding revision
    * @param pipeline
    *   the list of operations to apply on a resource before indexing
    * @param mapping
    *   the elasticsearch mapping to be used in order to create the index
    * @param settings
    *   the elasticsearch optional settings to be used in order to create the index
    * @param context
    *   an optional context to apply when compacting during the creation of the document to index
    * @param permission
    *   the permission required for querying this view
    */
  final case class IndexingElasticSearchViewValue(
      name: Option[String],
      description: Option[String],
      resourceTag: Option[UserTag] = None,
      pipeline: List[PipeStep] = defaultPipeline,
      mapping: Option[JsonObject] = None,
      settings: Option[JsonObject] = None,
      context: Option[ContextObject] = None,
      permission: Permission = permissions.query
  ) extends ElasticSearchViewValue {
    override val tpe: ElasticSearchViewType = ElasticSearchViewType.ElasticSearch

    /**
      * Translates the view into a [[PipeChain]]
      */
    def pipeChain: Option[PipeChain] =
      NonEmptyChain.fromSeq(pipeline).map { steps =>
        val pipes = steps.map { step =>
          (PipeRef(step.name), step.config.getOrElse(ExpandedJsonLd.empty))
        }
        PipeChain(pipes)
      }

    /**
      * Creates a [[SelectFilter]] for this view
      */
    def selectFilter: SelectFilter = {
      val types = pipeline
        .collectFirst {
          case PipeStep(label, _, Some(config)) if label == FilterByType.ref.label =>
            val filterByTypeConfig = JsonLdDecoder[FilterByTypeConfig].apply(config)
            filterByTypeConfig.map(_.types).getOrElse(IriFilter.None)
        }
        .getOrElse(IriFilter.None)
      SelectFilter(types, resourceTag.getOrElse(Latest))
    }

    /**
      * Returns true if this [[IndexingElasticSearchViewValue]] is equal to the provided
      * [[IndexingElasticSearchViewValue]] on the fields which should trigger a reindexing of the view when modified.
      */
    private def hasSameIndexingFields(that: IndexingElasticSearchViewValue): Boolean =
      resourceTag == that.resourceTag &&
        pipeline == that.pipeline &&
        mapping == that.mapping &&
        settings == that.settings &&
        context == that.context
  }

  object IndexingElasticSearchViewValue {

    /**
      * Default pipeline to apply if none is present in the payload
      */
    val defaultPipeline: List[PipeStep] = List(
      PipeStep(FilterDeprecated.ref.label, None, None),
      PipeStep(DiscardMetadata.ref.label, None, None),
      PipeStep(DefaultLabelPredicates.ref.label, None, None)
    )

    /**
      * @return
      *   an IndexingElasticSearchViewValue without name and description
      */
    def apply(
        resourceTag: Option[UserTag],
        pipeline: List[PipeStep],
        mapping: Option[JsonObject],
        settings: Option[JsonObject],
        context: Option[ContextObject],
        permission: Permission
    ): IndexingElasticSearchViewValue =
      IndexingElasticSearchViewValue(None, None, resourceTag, pipeline, mapping, settings, context, permission)

    /**
      * @return
      *   the next indexing revision based on the differences between the given views
      */
    def nextIndexingRev(
        view1: ElasticSearchViewValue,
        view2: ElasticSearchViewValue,
        currentIndexingRev: IndexingRev,
        newEventRev: Int
    ): IndexingRev =
      (view1.asIndexingValue, view2.asIndexingValue)
        .mapN { case (v1, v2) =>
          if (!v1.hasSameIndexingFields(v2)) IndexingRev(newEventRev)
          else currentIndexingRev
        }
        .getOrElse(currentIndexingRev)
  }

  /**
    * The configuration of the ElasticSearch view that delegates queries to multiple indices.
    *
    * @param views
    *   the collection of views where queries will be delegated (if necessary permissions are met)
    */
  final case class AggregateElasticSearchViewValue(
      name: Option[String],
      description: Option[String],
      views: NonEmptySet[ViewRef]
  ) extends ElasticSearchViewValue {
    override val tpe: ElasticSearchViewType = ElasticSearchViewType.AggregateElasticSearch
  }

  object AggregateElasticSearchViewValue {

    /**
      * @return
      *   an AggregateElasticSearchViewValue without name and description
      */
    def apply(views: NonEmptySet[ViewRef]): AggregateElasticSearchViewValue =
      AggregateElasticSearchViewValue(None, None, views)
  }

  object Source {

    implicit final val elasticSearchViewValueEncoder: Encoder.AsObject[ElasticSearchViewValue] = {
      import io.circe.generic.extras.Configuration
      import io.circe.generic.extras.semiauto.*
      implicit val config: Configuration = Configuration(
        transformMemberNames = identity,
        transformConstructorNames = {
          case "IndexingElasticSearchViewValue"  => ElasticSearchViewType.ElasticSearch.toString
          case "AggregateElasticSearchViewValue" => ElasticSearchViewType.AggregateElasticSearch.toString
          case other                             => other
        },
        useDefaults = false,
        discriminator = Some(keywords.tpe),
        strictDecoding = false
      )
      deriveConfiguredEncoder[ElasticSearchViewValue]
    }
  }

  object Database {
    implicit private val configuration: Configuration               = Serializer.circeConfiguration
    implicit val valueCodec: Codec.AsObject[ElasticSearchViewValue] = deriveConfiguredCodec[ElasticSearchViewValue]
  }

}
