package ai.senscience.nexus.delta.rdf.jsonld

import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.RdfError.ConversionError
import ai.senscience.nexus.delta.rdf.Vocabulary.schema
import ai.senscience.nexus.delta.rdf.graph.GraphAssertions
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, JsonLdOptions, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, Contexts, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.{Fixtures, GraphHelpers, RdfLoader}
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.syntax.EncoderOps

class ExpandedJsonLdSuite extends NexusSuite with RdfLoader with Fixtures with GraphHelpers with GraphAssertions {

  given JsonLdApi = TitaniumJsonLdApi.strict(
    JsonLdOptions(base = Some(iri"http://senscience.ai/"))
  )

  private val prefix = "http://senscience.ai"

  private val ctx: ContextValue = Contexts.value

  test("Construct an expanded JSON-LD") {
    expandedFromJson("compacted.json").flatMap { result =>
      expanded("expanded.json", "id" -> iri).map { expected =>
        assertEquals(result, expected)
      }
    }
  }

  test("Constructed successfully with a base defined in JsonLdOptions") {
    for {
      result   <- expandedFromJson("compacted-no-base.json")
      expected <- expanded("expanded-no-base.json")
    } yield {
      assertEquals(result, expected)
    }
  }

  test("Construct without @id") {
    val name             = vocab + "name"
    val expectedExpanded = json"""[{"@type": ["${schema.Person}"], "$name": [{"@value": "Me"} ] } ]"""
    val compacted        = json"""{"@type": "Person", "name": "Me"}""".addContext(ctx.contextObj.asJson)
    ExpandedJsonLd(compacted).map { expanded =>
      assertEquals(expanded.json, expectedExpanded)
      assert(expanded.rootId.isInstanceOf[BNode])
    }
  }

  test("Construct successfully with remote contexts") {
    for {
      result   <- expandedFromJson("jsonld/expanded/input-with-remote-context.json")
      expected <- expanded("expanded.json", "id" -> iri)
    } yield {
      assertEquals(result, expected)
    }
  }

  test("Construct successfully with injected @id") {
    val newId = iri"http://nexus.senscience.ai/new"
    for {
      result   <- expandedFromJson("compacted.json").map(_.replaceId(newId))
      expected <- expanded("expanded.json", "id" -> newId)
    } yield {
      assertEquals(result, expected)
    }
  }

  test("Constract an empty expanded result") {
    ExpandedJsonLd(json"""{"@id": "$iri"}""").map { expanded =>
      assertEquals(expanded.rootId, iri)
      assertEquals(expanded.json, json"""[ {} ]""")
    }
  }

  test("Constructed with multiple root objects") {
    val batmanIri = iri"$prefix/batman"
    val expected  = json"""[{"${keywords.graph}": [
              {"@id": "$iri", "@type": ["$prefix/Person"], "$prefix/name": [{"@value": "John"} ] },
               {"@id": "$batmanIri", "@type": ["$prefix/Person", "$prefix/Hero"], "$prefix/name": [{"@value": "Batman"} ] }
              ]}]"""
    expandedFromJson("jsonld/expanded/input-multiple-roots.json").map { result =>
      assertEquals(result.json, expected)
    }
  }

  test("Convert to compacted form") {
    for {
      result   <- expandedFromJson("compacted.json").flatMap(_.toCompacted(ctx))
      expected <- loader.jsonContentOf("compacted.json")
    } yield {
      assertEquals(result.json, expected)
    }
  }

  test("Convert to compacted form without id") {
    val json = json"""{"@type": "Person", "name": "Me"}""".addContext(ctx.contextObj.asJson)
    for {
      expanded  <- ExpandedJsonLd(json)
      compacted <- expanded.toCompacted(ctx)
    } yield {
      assertEquals(compacted.rootId, expanded.rootId)
      assertEquals(compacted.json, json)
    }
  }

  test("Be empty") {
    ExpandedJsonLd(json"""[{"@id": "$prefix/id", "a": "b"}]""")
      .assert(_.isEmpty)
  }

  test("Not be empty") {
    expandedFromJson("compacted.json")
      .assert(!_.isEmpty)
  }

  test("Be converted to graph") {
    for {
      graph    <- graphFromJson("compacted.json")
      expected <- graphFromJson("expanded.json", "id" -> iri)
    } yield {
      assertEquals(graph.rootNode, iri)
      assertIsomorphic(graph, expected)
    }
  }

  test("Add @id value") {
    val friends = vocab + "friends"
    val batman  = base + "batman"
    val robin   = base + "robin"
    ExpandedJsonLd
      .expanded(json"""[{"@id": "$iri"}]""")
      .map { expanded =>
        expanded.add(friends, batman).add(friends, robin).json
      }
      .assertRight(json"""[{"@id": "$iri", "$friends": [{"@id": "$batman"}, {"@id": "$robin"} ] } ]""")
  }

  test("Add @type Iri to existing @type") {
    val (person, animal, hero) = (schema.Person, schema + "Animal", schema + "Hero")
    ExpandedJsonLd
      .expanded(json"""[{"@id": "$iri", "@type": ["$person", "$animal"] } ]""")
      .map {
        _.addType(hero).addType(hero).json
      }
      .assertRight(json"""[{"@id": "$iri", "@type": ["$person", "$animal", "$hero"] } ]""")
  }

  test("Add @type Iri") {
    ExpandedJsonLd
      .expanded(json"""[{"@id": "$iri"}]""")
      .map {
        _.addType(schema.Person).json
      }
      .assertRight(json"""[{"@id": "$iri", "@type": ["${schema.Person}"] } ]""")
  }

  test("Add @value value") {
    val tags                           = vocab + "tags"
    val (tag1, tag2, tag3, tag4, tag5) = ("first", 2, false, 30L, 3.14)
    val expected                       =
      json"""[{"@id": "$iri", "$tags": [{"@value": "$tag1"}, {"@value": $tag2 }, {"@value": $tag3}, {"@value": $tag4}, {"@value": $tag5 } ] } ]"""

    ExpandedJsonLd
      .expanded(json"""[{"@id": "$iri"}]""")
      .map {
        _.add(tags, tag1)
          .add(tags, tag2)
          .add(tags, tag3)
          .add(tags, tag4)
          .add(tags, tag5)
          .json
      }
      .assertRight(expected)
  }

  test("Fail when there are remote cyclic references") {
    val contexts: Map[Iri, ContextValue]                   =
      Map(
        iri"http://localhost/c" -> ContextValue(json"""["http://localhost/d", {"c": "http://localhost/c"} ]"""),
        iri"http://localhost/d" -> ContextValue(json"""["http://localhost/e", {"d": "http://localhost/d"} ]"""),
        iri"http://localhost/e" -> ContextValue(json"""["http://localhost/c", {"e": "http://localhost/e"} ]""")
      )
    implicit val remoteResolution: RemoteContextResolution = RemoteContextResolution.fixed(contexts.toSeq*)

    val input =
      json"""{"@context": ["http://localhost/c", {"a": "http://localhost/a"} ], "a": "A", "c": "C", "d": "D"}"""

    ExpandedJsonLd(input).intercept[ConversionError].assert(_.getMessage.contains("Too many contexts"))
  }
}
