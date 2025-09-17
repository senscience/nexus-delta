package ai.senscience.nexus.delta.plugins.blazegraph.client

import ai.senscience.nexus.delta.kernel.RdfHttp4sMediaTypes.*
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponse.{SparqlJsonLdResponse, SparqlNTriplesResponse, SparqlRdfXmlResponse, SparqlResultsResponse, SparqlXmlResultsResponse}
import cats.data.NonEmptyList
import cats.syntax.all.*
import org.http4s.MediaType

/**
  * Enumeration of supported sparql query response types
  */
sealed trait SparqlQueryResponseType extends Product with Serializable {

  /**
    * @return
    *   the media types supported by this response type
    */
  def mediaTypes: NonEmptyList[MediaType]

  type R <: SparqlQueryResponse
}

object SparqlQueryResponseType {

  type Aux[R0] = SparqlQueryResponseType { type R = R0 }
  type Generic = SparqlQueryResponseType { type R = SparqlQueryResponse }

  private val `text/plain(UTF-8)` =
    new MediaType("text", "plain", compressible = true, binary = false, fileExtensions = List("nt", "txt"))

  /**
    * Constructor helper that creates a [[SparqlQueryResponseType]] from the passed ''mediaType''
    */
  def fromMediaType(mediaType: MediaType): Option[SparqlQueryResponseType] = {
    if SparqlResultsJson.mediaTypes.contains_(mediaType) then Some(SparqlResultsJson)
    else if SparqlResultsXml.mediaTypes.contains_(mediaType) then Some(SparqlResultsXml)
    else if SparqlJsonLd.mediaTypes.contains_(mediaType) then Some(SparqlJsonLd)
    else if SparqlNTriples.mediaTypes.contains_(mediaType) then Some(SparqlNTriples)
    else if SparqlRdfXml.mediaTypes.contains_(mediaType) then Some(SparqlRdfXml)
    else None
  }

  case object SparqlResultsJson extends SparqlQueryResponseType {
    override type R = SparqlResultsResponse
    override val mediaTypes: NonEmptyList[MediaType] = NonEmptyList.of(`application/sparql-results+json`)
  }

  case object SparqlResultsXml extends SparqlQueryResponseType {
    override type R = SparqlXmlResultsResponse
    override val mediaTypes: NonEmptyList[MediaType] = NonEmptyList.of(`application/sparql-results+xml`)
  }

  case object SparqlJsonLd extends SparqlQueryResponseType {
    override type R = SparqlJsonLdResponse
    override val mediaTypes: NonEmptyList[MediaType] = NonEmptyList.of(`application/ld+json`)
  }

  case object SparqlNTriples extends SparqlQueryResponseType {
    override type R = SparqlNTriplesResponse
    override val mediaTypes: NonEmptyList[MediaType] =
      NonEmptyList.of(`text/plain(UTF-8)`, `application/n-triples`)
  }

  case object SparqlRdfXml extends SparqlQueryResponseType {
    override type R = SparqlRdfXmlResponse
    override val mediaTypes: NonEmptyList[MediaType] = NonEmptyList.of(`application/rdf+xml`)
  }
}
