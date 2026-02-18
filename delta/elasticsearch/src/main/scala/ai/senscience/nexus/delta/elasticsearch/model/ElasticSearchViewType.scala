package ai.senscience.nexus.delta.elasticsearch.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.{Decoder, Encoder, Json}

/**
  * Enumeration of ElasticSearch view types.
  */
enum ElasticSearchViewType(suffix: String) {
  override val toString: String = suffix
  def tpe: Iri                  = nxv + toString
  def types: Set[Iri]           = Set(tpe, nxv + "View")

  /**
    * ElasticSearch view that indexes resources as documents.
    */
  case ElasticSearch extends ElasticSearchViewType("ElasticSearchView")

  /**
    * ElasticSearch view that delegates queries to a collection of existing ElasticSearch views based on access.
    */
  case AggregateElasticSearch extends ElasticSearchViewType("AggregateElasticSearchView")
}

object ElasticSearchViewType {

  given Encoder[ElasticSearchViewType] = Encoder.instance {
    case ElasticSearch          => Json.fromString("ElasticSearchView")
    case AggregateElasticSearch => Json.fromString("AggregateElasticSearchView")
  }

  given Decoder[ElasticSearchViewType] = Decoder.decodeString.emap {
    case "ElasticSearchView"          => Right(ElasticSearch)
    case "AggregateElasticSearchView" => Right(AggregateElasticSearch)
  }
}
