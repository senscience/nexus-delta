package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.Encoder

/**
  * Enumeration of composite view source types
  */
sealed trait ProjectionType {

  /**
    * @return
    *   the type id
    */
  def tpe: Iri

  /**
    * @return
    *   the full set of types
    */
  def types: Set[Iri] = Set(tpe, nxv + "CompositeViewProjection")
}

object ProjectionType {

  /**
    * ElasticSearch projection.
    */
  case object ElasticSearchProjectionType extends ProjectionType {

    override val toString: String = "ElasticSearchProjection"

    override def tpe: Iri = nxv + toString
  }

  /**
    * SPARQL projection.
    */
  case object SparqlProjectionType extends ProjectionType {

    override val toString: String = "SparqlProjection"

    override def tpe: Iri = nxv + toString
  }

  given Encoder[ProjectionType] = Encoder.encodeString.contramap(_.tpe.toString)
}
