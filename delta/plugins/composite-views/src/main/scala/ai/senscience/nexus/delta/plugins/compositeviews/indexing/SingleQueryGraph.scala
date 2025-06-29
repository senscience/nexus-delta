package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.SparqlNTriples
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.idTemplating
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import cats.effect.IO

import java.util.regex.Pattern.quote

/**
  * Provides a way to query for the incoming resource and replaces the graph with the result of query
  *
  * @param client
  *   the blazegraph client used to query
  * @param namespace
  *   the namespace to query
  * @param query
  *   the query to perform on each resource
  */
final class SingleQueryGraph(client: SparqlClient, namespace: String, query: SparqlConstructQuery) {

  private val logger = Logger[SingleQueryGraph]

  def apply(graphResource: GraphResource): IO[Option[GraphResource]] =
    for {
      ntriples    <- client.query(Set(namespace), replaceId(query, graphResource.id), SparqlNTriples)
      graphResult <- NTripleParser(ntriples.value, Some(graphResource.id))
      _           <- IO.whenA(graphResult.isEmpty)(
                       logger.debug(s"Querying blazegraph did not return any triples, '$graphResource' will be dropped.")
                     )
    } yield graphResult.map(g => graphResource.copy(graph = g))

  private def replaceId(query: SparqlConstructQuery, iri: Iri): SparqlConstructQuery =
    SparqlConstructQuery.unsafe(query.value.replaceAll(quote(idTemplating), s"<$iri>"))
}
