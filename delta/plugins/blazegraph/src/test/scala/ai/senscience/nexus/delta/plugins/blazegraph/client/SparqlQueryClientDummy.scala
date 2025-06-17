package ai.senscience.nexus.delta.plugins.blazegraph.client

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryClientDummy.bNode
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponse.*
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.*
import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import cats.effect.IO
import io.circe.Json
import org.http4s.Header

import scala.xml.NodeSeq

class SparqlQueryClientDummy(
    sparqlResults: Iterable[String] => SparqlResults = _ => SparqlResults.empty,
    sparqlResultsXml: Iterable[String] => NodeSeq = _ => NodeSeq.Empty,
    sparqlJsonLd: Iterable[String] => Json = _ => Json.obj(),
    sparqlNTriples: Iterable[String] => NTriples = _ => NTriples("", bNode),
    sparqlRdfXml: Iterable[String] => NodeSeq = _ => NodeSeq.Empty
) extends SparqlQueryClient {
  override def query[R <: SparqlQueryResponse](
      namespaces: Iterable[String],
      q: SparqlQuery,
      responseType: Aux[R],
      additionalHeaders: Seq[Header.ToRaw] = Seq.empty
  ): IO[R] =
    responseType match {
      case SparqlResultsJson =>
        IO.pure(SparqlResultsResponse(sparqlResults(namespaces)))
      case SparqlResultsXml  => IO.pure(SparqlXmlResultsResponse(sparqlResultsXml(namespaces)))
      case SparqlJsonLd      => IO.pure(SparqlJsonLdResponse(sparqlJsonLd(namespaces)))
      case SparqlNTriples    => IO.pure(SparqlNTriplesResponse(sparqlNTriples(namespaces)))
      case SparqlRdfXml      => IO.pure(SparqlRdfXmlResponse(sparqlRdfXml(namespaces)))
    }

}

object SparqlQueryClientDummy {
  val bNode = BNode.random
}
