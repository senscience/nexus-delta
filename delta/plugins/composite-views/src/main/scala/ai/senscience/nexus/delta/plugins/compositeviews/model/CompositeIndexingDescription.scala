package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeIndexingDescription.ProjectionSpace
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import cats.data.NonEmptyMap
import io.circe.Encoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder

/**
  * Describes the indexing context of a composite view
  * @param projectionName
  *   the name of the projection
  * @param commonSpace
  *   the name of the common blazegraph namespace
  * @param projectionSpaces
  *   the namespaces and indices for the different projections
  * @param offsets
  *   the offsets
  * @param statistics
  *   the statistics
  */
final case class CompositeIndexingDescription(
    projectionName: String,
    commonSpace: String,
    projectionSpaces: NonEmptyMap[Iri, ProjectionSpace],
    offsets: Vector[ProjectionOffset],
    statistics: Vector[ProjectionStatistics]
)

object CompositeIndexingDescription {

  sealed trait ProjectionSpace extends Product with Serializable

  object ProjectionSpace {

    final case class ElasticSearchSpace(value: String) extends ProjectionSpace

    final case class SparqlSpace(value: String) extends ProjectionSpace
  }

  implicit val compositeIndexingDescriptionEncoder: Encoder.AsObject[CompositeIndexingDescription] = {
    implicit val configuration: Configuration                              = Configuration.default.withDiscriminator(keywords.tpe)
    implicit val projectionSpaceEncoder: Encoder.AsObject[ProjectionSpace] = deriveConfiguredEncoder[ProjectionSpace]
    deriveConfiguredEncoder[CompositeIndexingDescription]
  }

  implicit val compositeIndexingDescriptionJsonLdEncoder: JsonLdEncoder[CompositeIndexingDescription] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.offset).merge(ContextValue(contexts.statistics)))

  implicit val compositeIndexingDescriptionHttpResponseFields: HttpResponseFields[CompositeIndexingDescription] =
    HttpResponseFields.defaultOk

}
