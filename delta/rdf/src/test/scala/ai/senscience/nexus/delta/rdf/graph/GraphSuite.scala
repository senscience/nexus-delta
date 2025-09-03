package ai.senscience.nexus.delta.rdf.graph

import ai.senscience.nexus.delta.rdf.Fixtures.*
import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.RdfError.{ConversionError, UnexpectedJsonLd}
import ai.senscience.nexus.delta.rdf.Triple.{obj, predicate, subject, Triple}
import ai.senscience.nexus.delta.rdf.Vocabulary.schema
import ai.senscience.nexus.delta.rdf.graph.Graph.rdfType
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdOptions, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.{ContextEmpty, ContextObject}
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.rdf.{GraphHelpers, RdfLoader}
import ai.senscience.nexus.testkit.mu.{JsonAssertions, NexusSuite, StringAssertions}
import cats.effect.IO
import io.circe.Json
import org.apache.jena.graph.Node

class GraphSuite
    extends NexusSuite
    with RdfLoader
    with GraphAssertions
    with GraphHelpers
    with StringAssertions
    with JsonAssertions {

  private def loadGraphFromExpanded(removeId: Boolean = false) = {
    val id = Option.unless(removeId)("id" -> iri)
    graphFromJson("expanded.json", id.toSeq*)
  }

  private def loadNamedGraphFromExpanded = graphFromJson("graph/expanded-multiple-roots-namedgraph.json")

  private def loadContext = context("context.json")

  private def loadNquads = nquads(iri, "nquads.nq")

  private val iriSubject = subject(iri)
  private val name       = predicate(schema.name)
  private val birthDate  = predicate(schema + "birthDate")
  private val deprecated = predicate(schema + "deprecated")
  private val unknown    = predicate(schema + "none")

  test("Creates a graph from expanded jsonld") {
    loadGraphFromExpanded()
      .map(_.getDefaultGraphSize)
      .assertEquals(16)
  }

  test("Creates a graph from n-quads") {
    loadNquads
      .flatMap(nq => IO.fromEither(Graph(nq)))
      .map(_.getDefaultGraphSize)
      .assertEquals(16)
  }

  test("Creates a graph from expanded jsonld with a root blank node") {
    loadGraphFromExpanded(removeId = true)
      .map(_.getDefaultGraphSize)
      .assertEquals(16)
  }

  test("Replaces its own root node") {
    val newRootId  = iri"http://senscience.ai/newid"
    val newSubject = subject(newRootId)

    loadGraphFromExpanded().map { graph =>
      val newGraph        = graph.replaceRootNode(newRootId)
      val expectedTriples = graph.triples.map { case (s, p, o) => (if (s == iriSubject) newSubject else s, p, o) }

      assertEquals(newGraph.rootNode, newRootId)
      assertEquals(newGraph.triples, expectedTriples)
    }
  }

  test("Returns a filtered graph") {
    val triplesToKeep = List(
      (iriSubject, name, Node.ANY),
      (iriSubject, birthDate, Node.ANY),
      (iriSubject, deprecated, Node.ANY)
    )

    loadGraphFromExpanded().map { graph =>
      val result   = graph.filter(triplesToKeep)
      val expected = Set(
        (iriSubject, deprecated, obj(false)),
        (iriSubject, name, obj("John Doe")),
        (iriSubject, birthDate, obj("1999-04-09T20:00Z"))
      )
      assertEquals(result.triples, expected)
    }
  }

  test("Return an empty filtered graph") {
    val triplesToKeep = List(
      (iriSubject, unknown, Node.ANY)
    )
    loadGraphFromExpanded().map { graph =>
      assertEquals(graph.filter(triplesToKeep).triples, Set.empty[Triple])
    }
  }

  test("Return a triple for a known subject and predicate") {
    loadGraphFromExpanded().map { graph =>
      val result = graph.find { case (s, p, _) => s == iriSubject && p == deprecated }
      assertEquals(result, Some((iriSubject, deprecated, obj(false))))
    }
  }

  test("Return none for a unknown predicate") {
    loadGraphFromExpanded().map { graph =>
      val result = graph.find { case (s, p, _) => s == iriSubject && p == unknown }
      assertEquals(result, None)
    }
  }

  test("Return the root @type fields") {
    loadGraphFromExpanded()
      .map(_.rootTypes)
      .assertEquals(Set(schema.Person))
  }

  test("Return a new graph with added triples") {
    loadGraphFromExpanded().map { graph =>
      val expected = graph.triples + ((iriSubject, schema.age, 30))
      val result   = graph.add(schema.age, 30)
      assertEquals(result.triples, expected)
    }
  }

  test("Be converted to NTriples and back to an isomorphic graph") {
    for {
      originalGraph <- loadGraphFromExpanded()
      ntriples      <- originalGraph.toNTriples
      newGraph      <- IO.fromEither(Graph(ntriples))
    } yield {
      assertIsomorphic(newGraph, originalGraph)
    }
  }

  test("Be converted to NQuads and back to an isomorphic graph") {
    for {
      originalGraph <- loadGraphFromExpanded()
      nquads        <- originalGraph.toNQuads
      newGraph      <- IO.fromEither(Graph(nquads))
    } yield {
      assertIsomorphic(newGraph, originalGraph)
    }
  }

  test("Be converted to NQuads and back to an isomorphic graph with a root blank node") {
    for {
      originalGraph <- loadGraphFromExpanded(removeId = true)
      nquads        <- originalGraph.toNQuads
      newGraph      <- IO.fromEither(Graph(nquads))
    } yield {
      assertIsomorphic(newGraph, originalGraph)
    }
  }

  test("Be converted to NQuads and back with a named graph") {
    for {
      originalGraph <- loadNamedGraphFromExpanded
      nquads        <- originalGraph.toNQuads
      newGraph      <- IO.fromEither(Graph(nquads))
    } yield {
      assertIsomorphic(newGraph, originalGraph)
    }
  }

  private def loadExpandedDot(bnode: BNode, rootNode: String) =
    loader.contentOf("graph/dot-expanded.dot", "bnode" -> bnode.rdfFormat, "rootNode" -> rootNode)

  private def loadCompactedDot(bnode: BNode, rootNode: String) =
    loader.contentOf("graph/dot-compacted.dot", "bnode" -> bnode.rdfFormat, "rootNode" -> rootNode)

  test("Be converted to dot without and without a context") {
    for {
      graph             <- loadGraphFromExpanded()
      bnode              = addressBNode(graph)
      expandedDot       <- graph.toDot()
      expectedExpanded  <- loadExpandedDot(bnode, iri.toString)
      context           <- loadContext
      compactedDot      <- graph.toDot(context)
      expectedCompacted <- loadCompactedDot(bnode, "john-doé")
    } yield {
      expandedDot.toString.equalLinesUnordered(expectedExpanded)
      compactedDot.toString.equalLinesUnordered(expectedCompacted)
    }
  }

  test("Be converted to dot without and without a context with a root blank node") {
    for {
      graph             <- loadGraphFromExpanded(removeId = true)
      bnode              = addressBNode(graph)
      expandedDot       <- graph.toDot()
      expectedExpanded  <- loadExpandedDot(bnode, graph.rootNode.rdfFormat)
      context           <- loadContext
      compactedDot      <- graph.toDot(context)
      expectedCompacted <- loadCompactedDot(bnode, graph.rootNode.rdfFormat)
    } yield {
      expandedDot.toString.equalLinesUnordered(expectedExpanded)
      compactedDot.toString.equalLinesUnordered(expectedCompacted)
    }
  }

  test("Fail to convert to a graph from a multiple root") {
    graphFromJson("graph/expanded-multiple-roots.json")
      .interceptEquals(UnexpectedJsonLd("Expected named graph, but root @id not found"))
  }

  test("Convert to compacted JSON-LD from a named graph") {
    val ctx = ContextObject(jobj"""{"@vocab": "http://schema.org/", "@base": "http://nexus.senscience.ai/"}""")
    for {
      graph       <- loadNamedGraphFromExpanded
      compacted   <- graph.toCompactedJsonLd(ctx)
      expectedRoot = iri"http://nexus.senscience.ai/named-graph"
      expected    <- compactedUnsafe(expectedRoot, ctx, "jsonld/graph/compacted-multiple-roots-namedgraph.json")
    } yield {
      assertEquals(compacted.rootId, expected.rootId)
      assertEquals(compacted.ctx, expected.ctx)
      compacted.json.equalsIgnoreArrayOrder(expected.json)
    }
  }

  test("Convert to compacted JSON-LD from an empty graph") {
    Graph.empty.toCompactedJsonLd(ContextEmpty).map(_.json).assertEquals(Json.obj())
  }

  // The returned json is not exactly the same as the original compacted json from where the Graph was created.
  // This is expected due to the useNativeTypes field on JsonLdOptions and to the @context we have set in place
  test("be converted to compacted JSON-LD") {
    for {
      graph     <- loadGraphFromExpanded()
      context   <- loadContext
      compacted <- graph.toCompactedJsonLd(context)
      expected  <- loader.jsonContentOf("graph/compacted.json")
    } yield {
      assertEquals(compacted.json, expected)
    }
  }

  // The returned json is not exactly the same as the original compacted json from where the Graph was created.
  // This is expected due to the useNativeTypes field on JsonLdOptions and to the @context we have set in place
  test("be converted to compacted JSON-LD with a root blank node") {
    for {
      graph     <- loadGraphFromExpanded(removeId = true)
      context   <- loadContext
      compacted <- graph.toCompactedJsonLd(context)
      expected  <- loader.jsonContentOf("graph/compacted.json").map(_.removeAll("id" -> "john-doé"))
    } yield {
      assertEquals(compacted.json, expected)
    }
  }

  test("return a new graph after running a construct query on it") {
    val query = SparqlConstructQuery.unsafe("""
      |prefix schema: <http://schema.org/>
      |
      |CONSTRUCT {
      |  ?person 	        a                       ?type ;
      |                   schema:name             ?name ;
      |                   schema:birthDate        ?birthDate ;
      |} WHERE {
      |  ?person 	        a                       ?type ;
      |         	        schema:name             ?name ;
      |                   schema:birthDate        ?birthDate ;
      |}
      |""".stripMargin)

    loadGraphFromExpanded().map { graph =>
      val tripleToKeep = List(
        (graph.rootResource, name, Node.ANY),
        (graph.rootResource, birthDate, Node.ANY),
        (graph.rootResource, rdfType, Node.ANY)
      )
      graph.transform(query).assertRight(graph.filter(tripleToKeep))
    }
  }

  test("Raise an error for an invalid query") {
    val query = SparqlConstructQuery.unsafe("""
      |prefix schema: <http://schema.org/>
      |
      |CONSTRUCT { fail }
      |""".stripMargin)
    loadGraphFromExpanded().map { graph =>
      graph.transform(query).assertLeft()
    }
  }

  test("Raise an error with a strict parser when an iri is invalid") {
    val expectedError = ConversionError(
      "Bad IRI: < http://nexus.senscience.ai/myid> Spaces are not legal in URIs/IRIs.",
      "toRdf"
    )
    graphFromJson("expanded-invalid-iri.json").interceptEquals(expectedError)
  }

  test("Not raise an error with a lenient parser when an iri is invalid") {
    expandedFromJson("expanded-invalid-iri.json")
      .flatMap { expanded =>
        Graph(expanded)(TitaniumJsonLdApi.lenient, JsonLdOptions.defaults)
      }
      .void
      .assert
  }
}
