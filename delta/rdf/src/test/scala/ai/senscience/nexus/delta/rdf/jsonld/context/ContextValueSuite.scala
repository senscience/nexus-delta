package ai.senscience.nexus.delta.rdf.jsonld.context

import ai.senscience.nexus.delta.rdf.Fixtures
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.delta.rdf.implicits.*
import io.circe.Json
import munit.Location

class ContextValueSuite extends NexusSuite with Fixtures {

  private def assertContext(json: Json, expectedContext: Json)(using Location): Unit =
    assertEquals(json.topContextValueOrEmpty, ContextValue(expectedContext))

  test("Be extracted from the @context field") {
    val expected = json"""{"@base": "${base.value}"}"""
    assertContext(json"""[{"@context": {"@base": "${base.value}"}, "@id": "$johnDoeIri", "age": 30}]""", expected)
    assertContext(json"""{"@context": {"@base": "${base.value}"}, "@id": "$johnDoeIri", "age": 30}""", expected)
  }

  test("Extract an empty context when there is no @context") {
    assertEquals(json"""{"@id": "$johnDoeIri", "age": 30}""".topContextValueOrEmpty, ContextValue.empty)
  }

  test("Be empty") {
    List(json"{}", json"[]", Json.fromString(""), Json.Null).foreach { json =>
      assert(ContextValue(json).isEmpty, s"Context should be empty from $json")
    }
  }

  test("Not be empty") {
    assertEquals(ContextValue(json"""{"@base": "${base.value}"}""").isEmpty, false)
  }

  test("Return its @context object") {
    assertEquals(
      ContextValue(json"""{"@base": "${base.value}"}""").contextObj,
      jobj"""{"@context": {"@base": "${base.value}"}}"""
    )
  }

  test("Prevent merging two times the same context") {
    val context = ContextValue(contexts.metadata)
    assertEquals(context.merge(context), context)
  }
}
