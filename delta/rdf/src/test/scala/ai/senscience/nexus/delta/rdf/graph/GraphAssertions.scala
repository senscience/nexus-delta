package ai.senscience.nexus.delta.rdf.graph

import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.Location

import scala.jdk.CollectionConverters.*

trait GraphAssertions { suite: NexusSuite =>

  def assertIsomorphic(left: Graph, right: Graph)(using Location): Unit = {

    left.rootNode match {
      case iri: Iri => assertEquals(Some(iri), right.rootNode.asIri)
      case _: BNode => assert(right.rootNode.isBNode, s"${right.rootNode} should be a BNode")
    }

    val leftDefaultGraph  = left.value.getDefaultGraph
    val rightDefaultGraph = right.value.getDefaultGraph

    def print(graph: org.apache.jena.graph.Graph) = {
      graph.find().asScala.toList.map(_.toString).sorted.mkString("\n")
    }

    if !leftDefaultGraph.isIsomorphicWith(rightDefaultGraph) then {
      fail(
        s"""
           |Default graphs should be isomorphic:
           |${print(leftDefaultGraph)}
           |------------------------
           |${print(rightDefaultGraph)}
           |""".stripMargin
      )
    }

    val leftGraphNodes  = left.value.listGraphNodes().asScala.toList
    val rightGraphNodes = right.value.listGraphNodes().asScala.toList

    assertEquals(leftGraphNodes, rightGraphNodes, "Both graph should have the same subgraphs")

    leftGraphNodes.foreach { graphNode =>
      val leftGraph  = left.value.getGraph(graphNode)
      val rightGraph = right.value.getGraph(graphNode)
      if !leftGraph.isIsomorphicWith(rightGraph) then {
        fail(
          s"""
             |'$graphNode' should be isomorphic in both graphs:
             |${print(leftGraph)}
             |------------------------
             |${print(rightGraph)}
             |""".stripMargin
        )
      }
    }
  }

}
