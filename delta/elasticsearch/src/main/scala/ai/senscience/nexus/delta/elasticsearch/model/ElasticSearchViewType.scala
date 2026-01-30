package ai.senscience.nexus.delta.elasticsearch.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.{Decoder, Encoder, Json}

/**
  * Enumeration of ElasticSearch view types.
  */
sealed trait ElasticSearchViewType extends Product with Serializable {

  /**
    * @return
    *   the type id
    */
  def tpe: Iri

  /**
    * @return
    *   the full set of types
    */
  def types: Set[Iri] = Set(tpe, nxv + "View")
}

object ElasticSearchViewType {

  /**
    * ElasticSearch view that indexes resources as documents.
    */
  case object ElasticSearch extends ElasticSearchViewType {
    override val toString: String = "ElasticSearchView"
    override val tpe: Iri         = nxv + toString
  }

  /**
    * ElasticSearch view that delegates queries to a collection of existing ElasticSearch views based on access.
    */
  case object AggregateElasticSearch extends ElasticSearchViewType {
    override val toString: String = "AggregateElasticSearchView"
    override val tpe: Iri         = nxv + toString
  }

  given Encoder[ElasticSearchViewType] = Encoder.instance {
    case ElasticSearch          => Json.fromString("ElasticSearchView")
    case AggregateElasticSearch => Json.fromString("AggregateElasticSearchView")
  }

  given Decoder[ElasticSearchViewType] = Decoder.decodeString.emap {
    case "ElasticSearchView"          => Right(ElasticSearch)
    case "AggregateElasticSearchView" => Right(AggregateElasticSearch)
  }
}
