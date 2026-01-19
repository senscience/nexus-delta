package ai.senscience.nexus.delta.rdf.jsonld.context

import ai.senscience.nexus.delta.rdf.Fixtures
import ai.senscience.nexus.delta.rdf.Vocabulary.{rdf, schema, xsd}
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.kernel.Resource
import munit.AnyFixture

class JsonLdContextSuite extends NexusSuite with Fixtures {

  private val context: ContextValue = Contexts.value

  private val jsonLdContext = ResourceSuiteLocalFixture(
    "jsonLdContext",
    Resource.eval(api.context(context))
  )

  private val baseContext = ContextValue(json"""{"@base": "${base.value}"} """)

  private val baseJsonLdContext = ResourceSuiteLocalFixture(
    "baseJsonLdContext",
    Resource.eval(api.context(baseContext))
  )

  override def munitFixtures: Seq[AnyFixture[?]] = List(jsonLdContext, baseJsonLdContext)

  test("Get fields") {
    val result = jsonLdContext()
    assertEquals(result.value, context)
    assertEquals(result.base, Some(base.value))
    assertEquals(result.vocab, Some(vocab.value))

    val aliases = Map(
      "id"         -> iri"@id",
      "Person"     -> schema.Person,
      "deprecated" -> (schema + "deprecated"),
      "customid"   -> (vocab + "customid")
    )
    assertEquals(result.aliases, aliases)

    val aliasesInv = Map(
      iri"@id"                -> "id",
      schema.Person           -> "Person",
      (schema + "deprecated") -> "deprecated",
      (vocab + "customid")    -> "customid"
    )
    assertEquals(result.aliasesInv, aliasesInv)

    assertEquals(result.prefixMappings, Map("schema" -> schema.base, "xsd" -> xsd.base))
    assertEquals(result.prefixMappingsInv, Map(schema.base -> "schema", xsd.base -> "xsd"))
  }

  test("Compact an iri to its short form using an alias") {
    assertEquals(jsonLdContext().alias(schema.Person), Some("Person"))
    assertEquals(jsonLdContext().alias(schema.age), None)
  }

  test("Compact an iri to a CURIE using a prefix mappings") {
    assertEquals(jsonLdContext().curie(schema.age), Some("schema:age"))
    assertEquals(jsonLdContext().curie(rdf.tpe), None)
  }

  test("Compact an iri to its short form using the vocab") {
    assertEquals(jsonLdContext().compactVocab(vocab + "name"), Some("name"))
    assertEquals(jsonLdContext().compactVocab(schema.age), None)
  }

  test("Compact an iri to its short form using the base") {
    assertEquals(jsonLdContext().compactBase(base + "name"), Some("name"))
    assertEquals(jsonLdContext().compactBase(schema.age), None)
  }

  test("Compact an iri using aliases, vocab and prefix mappings") {
    List(
      vocab + "name" -> "name",
      schema.Person  -> "Person",
      schema.age     -> "schema:age",
      rdf.tpe        -> rdf.tpe.toString
    ).foreach { case (iri, expected) =>
      assertEquals(jsonLdContext().compact(iri, useVocab = true), expected, s"Failed for $iri")
    }
  }

  test("Compact an iri using aliases, base and prefix mappings") {
    List(
      base + "name" -> "name",
      schema.Person -> "Person",
      schema.age    -> "schema:age",
      rdf.tpe       -> rdf.tpe.toString
    ).foreach { case (iri, expected) =>
      assertEquals(jsonLdContext().compact(iri, useVocab = false), expected, s"Failed for $iri")
    }
  }

  test("Add simple alias") {
    val expected = JsonLdContext(
      value = ContextValue(json"""{"@base": "${base.value}", "age": "${schema.age}"}"""),
      base = Some(base.value),
      aliases = Map("age" -> schema.age)
    )
    val result   = baseJsonLdContext().addAlias("age", schema.age)
    assertEquals(result, expected)
  }

  test("Add alias for dataType") {
    val expected = JsonLdContext(
      value =
        ContextValue(json"""{"@base": "${base.value}", "age": {"@type": "${xsd.integer}", "@id": "${schema.age}"}}"""),
      base = Some(base.value),
      aliases = Map("age" -> schema.age)
    )
    val result   = baseJsonLdContext().addAlias("age", schema.age, xsd.integer)
    assertEquals(result, expected)
  }

  test("Add alias with @type @id") {
    val expected = JsonLdContext(
      value = ContextValue(json"""{"@base": "${base.value}", "unit": {"@type": "@id", "@id": "${schema.unitText}"}}"""),
      base = Some(base.value),
      aliases = Map("unit" -> schema.unitText)
    )
    val result   = baseJsonLdContext().addAliasIdType("unit", schema.unitText)
    assertEquals(result, expected)
  }

  test("Add prefixMapping") {
    val expected = JsonLdContext(
      value = ContextValue(json"""{"@base": "${base.value}", "xsd": "${xsd.base}"}"""),
      base = Some(base.value),
      prefixMappings = Map("xsd" -> xsd.base)
    )
    val result   = baseJsonLdContext().addPrefix("xsd", xsd.base)
    assertEquals(result, expected)
  }

  test("Add remote contexts Iri") {
    val remoteCtx = iri"https://senscience.ai/remote"
    List(
      json"""{"@id":"$iri","age": 30}"""                                                                                    -> json"""{"@context":"$remoteCtx","@id":"$iri","age": 30}""",
      json"""{"@context":"http://ex.com/1","@id":"$iri","age": 30}"""                                                       -> json"""{"@context":["http://ex.com/1","$remoteCtx"],"@id":"$iri","age": 30}""",
      json"""{"@context":[],"@id":"$iri","age": 30}"""                                                                      -> json"""{"@context":"$remoteCtx","@id":"$iri","age": 30}""",
      json"""{"@context":{},"@id":"$iri","age": 30}"""                                                                      -> json"""{"@context":"$remoteCtx","@id":"$iri","age": 30}""",
      json"""{"@context":"","@id":"$iri","age": 30}"""                                                                      -> json"""{"@context":"$remoteCtx","@id":"$iri","age": 30}""",
      json"""{"@context":["http://ex.com/1","http://ex.com/2"],"@id":"$iri","age": 30}"""                                   -> json"""{"@context":["http://ex.com/1","http://ex.com/2","$remoteCtx"],"@id":"$iri","age": 30}""",
      json"""{"@context":{"@vocab":"${vocab.value}","@base":"${base.value}"},"@id":"$iri","age": 30}"""                     -> json"""{"@context":[{"@vocab":"${vocab.value}","@base":"${base.value}"},"$remoteCtx"],"@id":"$iri","age": 30}""",
      json"""{"@context":[{"@vocab":"${vocab.value}","@base":"${base.value}"},"http://ex.com/1"],"@id":"$iri","age": 30}""" -> json"""{"@context":[{"@vocab":"${vocab.value}","@base":"${base.value}"}, "http://ex.com/1", "$remoteCtx"],"@id":"$iri","age": 30}"""
    ).foreach { case (input, expected) =>
      assertEquals(input.addContext(remoteCtx), expected)
    }
  }

  test("Merge contexts") {
    val context1 = jsonContentOf("jsonld/context/context1.json")
    val context2 = jsonContentOf("jsonld/context/context2.json")
    val expected = jsonContentOf("jsonld/context/context12-merged.json")
    assertEquals(context1.addContext(context2), expected)

    val json1 = context1.deepMerge(json"""{"@id": "$iri", "age": 30}""")
    assertEquals(json1.addContext(context2), expected.deepMerge(json"""{"@id": "$iri", "age": 30}"""))
  }

  test("Merge contexts when one is empty") {
    val context1    = jsonContentOf("jsonld/context/context1.json")
    val emptyArrCtx = json"""{"@context": []}"""
    val emptyObjCtx = json"""{"@context": {}}"""
    assertEquals(context1.addContext(emptyArrCtx), context1)
    assertEquals(context1.addContext(emptyObjCtx), context1)
  }

  test("Merge remote context IRIs") {
    val remoteCtxIri1 = iri"http://example.com/remote1"
    val remoteCtxIri2 = iri"http://example.com/remote2"
    val remoteCtx1    = json"""{"@context": "$remoteCtxIri1"}"""
    val remoteCtx2    = json"""{"@context": "$remoteCtxIri2"}"""
    assertEquals(remoteCtx1.addContext(remoteCtx2), json"""{"@context": ["$remoteCtxIri1","$remoteCtxIri2"]}""")
  }

  test("Merge contexts with arrays") {
    val context1Array = jsonContentOf("jsonld/context/context1-array.json")
    val context2      = jsonContentOf("jsonld/context/context2.json")
    val expected      = jsonContentOf("jsonld/context/context12-merged-array.json")
    assertEquals(context1Array.addContext(context2), expected)

    val json1 = context1Array.deepMerge(json"""{"@id": "$iri", "age": 30}""")
    assertEquals(json1.addContext(context2), expected.deepMerge(json"""{"@id": "$iri", "age": 30}"""))
  }
}
