package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticsearchMappings, ElasticsearchSettings}
import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel.IndexGroup
import ai.senscience.nexus.delta.elasticsearch.indexing.GraphResourceToDocument
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.GraphResourceToNTriples
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.{ElasticSearchProjection, SparqlProjection}
import ai.senscience.nexus.delta.plugins.compositeviews.model.ProjectionType.{ElasticSearchProjectionType, SparqlProjectionType}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.IndexingRev
import ai.senscience.nexus.delta.sourcing.model.IriFilter
import ai.senscience.nexus.delta.sourcing.stream.{Operation, PipeChain}
import io.circe.Encoder

import java.util.UUID

/**
  * A target projection for [[CompositeView]].
  */
sealed trait CompositeViewProjection extends Product with Serializable {

  /**
    * @return
    *   the id of the projection
    */
  def id: Iri

  /**
    * @return
    *   the uuid of the projection
    */
  def uuid: UUID

  /**
    * @return
    *   the indexing revision of the projection
    */
  def indexingRev: IndexingRev

  /**
    * SPARQL query used to create values indexed into the projection.
    */
  def query: SparqlConstructQuery

  /**
    * @return
    *   the schemas to filter by, empty means all
    */
  def resourceSchemas: IriFilter

  /**
    * @return
    *   the resource types to filter by, empty means all
    */
  def resourceTypes: IriFilter

  /**
    * @return
    *   whether to include deprecated resources
    */
  def includeMetadata: Boolean

  /**
    * @return
    *   whether to include resource metadata
    */
  def includeDeprecated: Boolean

  /**
    * @return
    *   permission required to query the projection
    */
  def permission: Permission

  /**
    * @return
    *   the type of the projection
    */
  def tpe: ProjectionType

  /**
    * @return
    *   Some(projection) if the current projection is an [[SparqlProjection]], None otherwise
    */
  def asSparql: Option[SparqlProjection]

  /**
    * @return
    *   Some(projection) if the current projection is an [[ElasticSearchProjection]], None otherwise
    */
  def asElasticSearch: Option[ElasticSearchProjection]

  /**
    * Translates the projection into a [[PipeChain]]
    */
  def pipeChain: Option[PipeChain] = PipeChain(resourceSchemas, resourceTypes, includeMetadata, includeDeprecated)

  def transformationPipe(using RemoteContextResolution): Operation.Pipe

  def updateIndexingRev(value: IndexingRev): CompositeViewProjection =
    this match {
      case e: ElasticSearchProjection => e.copy(indexingRev = value)
      case s: SparqlProjection        => s.copy(indexingRev = value)
    }
}

object CompositeViewProjection {

  /**
    * The templating id for the projection query
    */
  val idTemplating = "{resource_id}"

  /**
    * An ElasticSearch projection for [[CompositeView]].
    */
  final case class ElasticSearchProjection(
      id: Iri,
      uuid: UUID,
      indexingRev: IndexingRev,
      query: SparqlConstructQuery,
      resourceSchemas: IriFilter,
      resourceTypes: IriFilter,
      includeMetadata: Boolean,
      includeDeprecated: Boolean,
      includeContext: Boolean,
      permission: Permission,
      indexGroup: Option[IndexGroup],
      mapping: ElasticsearchMappings,
      settings: Option[ElasticsearchSettings] = None,
      context: ContextObject
  ) extends CompositeViewProjection {

    override def tpe: ProjectionType                              = ElasticSearchProjectionType
    override def asSparql: Option[SparqlProjection]               = None
    override def asElasticSearch: Option[ElasticSearchProjection] = Some(this)

    override def transformationPipe(using RemoteContextResolution): Operation.Pipe = {
      given JsonLdApi = TitaniumJsonLdApi.lenient
      new GraphResourceToDocument(context, includeContext)
    }

    def indexDef: ElasticsearchIndexDef = ElasticsearchIndexDef(mapping, settings)
  }

  /**
    * A Sparql projection for [[CompositeView]].
    */
  final case class SparqlProjection(
      id: Iri,
      uuid: UUID,
      indexingRev: IndexingRev,
      query: SparqlConstructQuery,
      resourceSchemas: IriFilter,
      resourceTypes: IriFilter,
      includeMetadata: Boolean,
      includeDeprecated: Boolean,
      permission: Permission
  ) extends CompositeViewProjection {

    override def tpe: ProjectionType                              = SparqlProjectionType
    override def asSparql: Option[SparqlProjection]               = Some(this)
    override def asElasticSearch: Option[ElasticSearchProjection] = None

    override def transformationPipe(using RemoteContextResolution): Operation.Pipe = GraphResourceToNTriples
  }

  given Encoder.AsObject[CompositeViewProjection] = {
    import io.circe.generic.extras.Configuration
    import io.circe.generic.extras.semiauto.*
    given Configuration = Configuration(
      transformMemberNames = {
        case "id"  => keywords.id
        case other => other
      },
      transformConstructorNames = identity,
      useDefaults = false,
      discriminator = Some(keywords.tpe),
      strictDecoding = false
    )
    deriveConfiguredEncoder[CompositeViewProjection]
  }

}
