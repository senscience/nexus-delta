package ai.senscience.nexus.delta.rdf.jsonld

import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.implicits.{*, given}
import ai.senscience.nexus.delta.rdf.jsonld.JsonLdEncoderSuite.Permissions
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.testkit.mu.{NexusSuite, StringAssertions}
import io.circe.syntax.*
import io.circe.{Encoder, JsonObject}

class JsonLdEncoderSuite extends NexusSuite with StringAssertions {

  private val permissions        = Permissions(Set("read", "write", "execute"))
  private val permissionsContext = json"""{ "@context": {"permissions": "${nxv + "permissions"}"} }"""

  given JsonLdApi                                 = TitaniumJsonLdApi.strict
  given remoteResolution: RemoteContextResolution =
    RemoteContextResolution.fixed(contexts.permissions -> permissionsContext.topContextValueOrEmpty)

  test("return a compacted Json-LD format") {
    val compacted = json"""{ "@context": "${contexts.permissions}", "permissions": [ "read", "write", "execute" ] }"""
    permissions.toCompactedJsonLd.map(_.json).assertEquals(compacted)
  }

  test("return an expanded Json-LD format") {
    val expanded =
      json"""[{"${nxv + "permissions"}": [{"@value": "read"}, {"@value": "write"}, {"@value": "execute"} ] } ]"""
    permissions.toExpandedJsonLd.map(_.json).assertEquals(expanded)
  }

  test("return a DOT format") {
    def dot(rootNode: IriOrBNode) =
      s"""digraph "${rootNode.rdfFormat}" {
         |  "${rootNode.rdfFormat}" -> "execute" [label = "permissions"]
         |  "${rootNode.rdfFormat}" -> "write" [label = "permissions"]
         |  "${rootNode.rdfFormat}" -> "read" [label = "permissions"]
         |}""".stripMargin

    permissions.toDot.map { result =>
      result.toString.equalLinesUnordered(dot(result.rootNode))
    }
  }

  private def ntriples(rootNode: IriOrBNode) =
    s"""${rootNode.rdfFormat} <${nxv + "permissions"}> "execute" .
       |${rootNode.rdfFormat} <${nxv + "permissions"}> "write" .
       |${rootNode.rdfFormat} <${nxv + "permissions"}> "read" .
       |""".stripMargin

  test("return a NTriples format") {
    permissions.toNTriples.map { result =>
      result.toString.equalLinesUnordered(ntriples(result.rootNode))
    }
  }

  test("return a NQuads format") {
    permissions.toNQuads.map { result =>
      result.toString.equalLinesUnordered(ntriples(result.rootNode))
    }
  }

  test("return a graph") {
    def graph(rootNode: IriOrBNode) = Graph
      .empty(rootNode)
      .add(nxv + "permissions", "execute")
      .add(nxv + "permissions", "write")
      .add(nxv + "permissions", "read")

    permissions.toGraph.map { result =>
      assertEquals(result, graph(result.rootNode))
    }
  }
}

object JsonLdEncoderSuite {

  final case class Permissions(permissions: Set[String])

  object Permissions {
    given Encoder.AsObject[Permissions] =
      Encoder.AsObject.instance(p => JsonObject.empty.add("permissions", p.permissions.asJson))

    given JsonLdEncoder[Permissions] =
      JsonLdEncoder.computeFromCirce(id = BNode.random, ctx = ContextValue(contexts.permissions))
  }
}
