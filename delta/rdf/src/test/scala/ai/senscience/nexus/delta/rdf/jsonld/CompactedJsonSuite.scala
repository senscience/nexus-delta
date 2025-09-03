package ai.senscience.nexus.delta.rdf.jsonld

import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.graph.GraphAssertions
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.{Fixtures, IriOrBNode, RdfLoader}
import ai.senscience.nexus.testkit.mu.NexusSuite

class CompactedJsonSuite extends NexusSuite with Fixtures with RdfLoader with GraphAssertions {

  private def loadContext = context("context.json")

  private def loadCompactedFromExpanded(rootId: IriOrBNode, removeId: Boolean = false) = {
    val id = Option.unless(removeId)("id" -> iri)
    loadContext.flatMap { ctx =>
      compactedFromJson(rootId, ctx, "expanded.json", id.toSeq*)
    }
  }

  private val rootBNode = BNode.random

  test("Construct successfully") {
    for {
      result          <- loadCompactedFromExpanded(iri)
      expectedContext <- loadContext
      expectedJson    <- loader.jsonContentOf("compacted.json")
    } yield {
      assertEquals(result.rootId, iri)
      assertEquals(result.ctx, expectedContext)
      assertEquals(result.json, expectedJson)
    }
  }

  test("Construct successfully with a root blank node") {
    for {
      result       <- loadCompactedFromExpanded(rootBNode)
      expectedJson <- loader.jsonContentOf("compacted.json")
    } yield {
      assertEquals(result.rootId, rootBNode)
      assertEquals(result.json, expectedJson)
    }
  }

  test("Construct from a multi-root json") {
    for {
      ctx    <- loadContext
      result <- compactedFromJson(rootBNode, ctx, "jsonld/compacted/input-multiple-roots.json")
    } yield {
      assertEquals(
        result.json.removeKeys(keywords.context),
        json"""{"@graph": [{"id": "john-doé", "@type": "Person"}, {"id": "batman", "@type": "schema:Hero"} ] }"""
      )
    }
  }

  test("Frame from a multi-root json") {
    for {
      ctx    <- loadContext
      json   <- loader.jsonContentOf("jsonld/compacted/input-multiple-roots.json")
      framed <- CompactedJsonLd.frame(iri, ctx, json)
    } yield {
      assertEquals(
        framed.json.removeKeys(keywords.context),
        json"""{"id": "john-doé", "@type": "Person"}"""
      )
    }
  }

  test("Convert to expanded form") {
    for {
      compacted <- loadCompactedFromExpanded(iri)
      result    <- compacted.toExpanded
      expected  <- expanded("expanded.json", "id" -> iri)
    } yield {
      assertEquals(result, expected)
    }
  }

  test("Convert to expanded form with a root blank node") {
    for {
      compacted <- loadCompactedFromExpanded(rootBNode)
      result    <- compacted.toExpanded
      expected  <- expanded("expanded.json").map(_.replaceId(rootBNode))
    } yield {
      assertEquals(result, expected)
    }
  }

  test("Convert to graph") {
    for {
      compacted <- loadCompactedFromExpanded(iri)
      result    <- compacted.toGraph
      expected  <- graphFromJson("expanded.json", "id" -> iri)
    } yield {
      assertIsomorphic(result, expected)
    }
  }

  test("Convert with a root blank node") {
    for {
      compacted <- loadCompactedFromExpanded(rootBNode)
      result    <- compacted.toGraph
      expected  <- graphFromJson("expanded.json")
    } yield {
      assertIsomorphic(result, expected)
    }
  }

  test("merge with another compacted document") {
    loadContext.map { context =>
      val compacted  = CompactedJsonLd.unsafe(rootBNode, context, jobj"""{"@type": "Person"}""")
      val compacted2 = CompactedJsonLd.unsafe(iri, ContextValue.empty, jobj"""{"name": "Batman"}""")

      val expectedMerge1and2 =
        CompactedJsonLd.unsafe(iri, context, jobj"""{"@id": "$iri", "@type": "Person", "name": "Batman"}""")
      assertEquals(compacted.merge(iri, compacted2), expectedMerge1and2)

      val expectedMerge2and1 =
        CompactedJsonLd.unsafe(rootBNode, context, jobj"""{"@type": "Person", "name": "Batman"}""")
      assertEquals(compacted2.merge(rootBNode, compacted), expectedMerge2and1)
    }
  }
}
