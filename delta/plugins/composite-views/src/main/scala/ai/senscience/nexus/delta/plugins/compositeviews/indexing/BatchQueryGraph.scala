package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.SparqlNTriples
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.idTemplating
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import cats.effect.IO
import fs2.Chunk

import java.util.regex.Pattern.quote

/**
  * Provides a way to query for the multiple incoming resources (from a chunk). This assumes that the query contains the
  * template: `VALUE ?id { {resource_id} }`. The result is a single Graph for all given resources.
  * @param client
  *   the SPARQL client used to query
  * @param namespace
  *   the namespace to query
  * @param query
  *   the sparql query to perform
  */
final class BatchQueryGraph(client: SparqlClient, namespace: String, query: SparqlConstructQuery) {

  private val logger = Logger[BatchQueryGraph]

  def apply(ids: Chunk[Iri]): IO[Option[Graph]] =
    for {
      ntriples    <- client.query(Set(namespace), replaceIds(query, ids), SparqlNTriples)
      graphResult <- NTripleParser(ntriples.value, None)
      _           <- IO.whenA(graphResult.isEmpty)(
                       logger.info(s"Querying blazegraph did not return any triples, '$ids' will be dropped.")
                     )
    } yield graphResult

  private def replaceIds(query: SparqlConstructQuery, iris: Chunk[Iri]): SparqlConstructQuery = {
    val replacement = iris.foldLeft("") { (acc, iri) => acc + " " + s"<$iri>" }
    SparqlConstructQuery.unsafe(query.value.replaceAll(quote(idTemplating), replacement))
  }

}
