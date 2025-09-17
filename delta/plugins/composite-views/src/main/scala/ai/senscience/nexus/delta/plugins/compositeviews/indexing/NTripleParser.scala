package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.RdfError.ParsingError
import ai.senscience.nexus.delta.rdf.graph.{Graph, NTriples}
import cats.effect.IO

object NTripleParser {

  private val logger = Logger[NTripleParser.type]

  def apply(ntriples: NTriples, rootNodeOpt: Option[Iri]): IO[Option[Graph]] =
    if ntriples.isEmpty then {
      // If nothing is returned by the query, we skip
      IO.none
    } else
      {
        rootNodeOpt match {
          case Some(rootNode) =>
            IO.fromEither(Graph(ntriples.copy(rootNode = rootNode))).map { g =>
              Some(g.replaceRootNode(rootNode))
            }
          case None           =>
            IO.fromEither(Graph(ntriples)).map(Some(_))
        }
      }.onError {
        case p: ParsingError =>
          logger.error(p)("Blazegraph did not send back valid n-triples, please check the responses")
        case _               => IO.unit
      }
}
