package ai.senscience.nexus.delta.plugins.blazegraph.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.{Decoder, Encoder, Json}

/**
  * Enumeration of Blazegraph view types.
  */
enum BlazegraphViewType(suffix: String) {
  override val toString: String = suffix
  def tpe: Iri                  = nxv + toString
  def types: Set[Iri]           = Set(tpe, nxv + "View")

  /**
    * Blazegraph view that indexes resources as triples.
    */
  case IndexingBlazegraphView extends BlazegraphViewType("SparqlView")

  /**
    * Blazegraph view that delegates queries to a collections of existing Blazegraph views based on access.
    */
  case AggregateBlazegraphView extends BlazegraphViewType("AggregateSparqlView")
}

object BlazegraphViewType {

  given Encoder[BlazegraphViewType] = Encoder.instance {
    case IndexingBlazegraphView  => Json.fromString("BlazegraphView")
    case AggregateBlazegraphView => Json.fromString("AggregateBlazegraphView")
  }

  given Decoder[BlazegraphViewType] = Decoder.decodeString.emap {
    case "BlazegraphView"          => Right(IndexingBlazegraphView)
    case "AggregateBlazegraphView" => Right(AggregateBlazegraphView)
  }
}
