package ai.senscience.nexus.delta.rdf.shacl

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.IO
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.shacl.Shapes
import org.apache.jena.sparql.graph.GraphFactory

object ShaclFileLoader {

  private val loader = ClasspathResourceLoader()

  private def readFromTurtleFile(resourcePath: String): IO[Shapes] =
    loader
      .streamOf(resourcePath)
      .use { is =>
        IO.blocking {
          val graph = GraphFactory.createDefaultGraph()
          RDFDataMgr.read(graph, is, Lang.TURTLE)
          Shapes.parse(graph)
        }
      }

  def readShaclShapes: IO[Shapes] = readFromTurtleFile("shacl-shacl.ttl")

  def readShaclVocabulary: IO[Shapes] = readFromTurtleFile("rdf/shacl.ttl")
}
