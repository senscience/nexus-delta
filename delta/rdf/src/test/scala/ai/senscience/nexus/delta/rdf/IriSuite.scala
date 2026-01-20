package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{owl, schema, xsd}
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.order.*
import io.circe.Json
import io.circe.syntax.*
import org.http4s.Query

class IriSuite extends NexusSuite {

  private val iriString = "http://senscience.ai/a"
  private val iri       = iri"$iriString"

  test("Fails to construct") {
    Iri.reference("abc").assertLeft()
    Iri.apply("a:*#").assertLeft()
  }

  test("Is a reference") {
    assert(iri.isReference)
  }

  test("Is a prefix mapping") {
    List(schema.base, xsd.base, owl.base).foreach { iri =>
      assert(iri.isPrefixMapping, s"$iri should be prefix mapping")
    }
  }

  test("Is NOT a prefix mapping") {
    List(schema.Person, xsd.int, owl.oneOf).foreach { iri =>
      assert(!iri.isPrefixMapping, s"$iri should be prefix mapping")
    }
  }

  test("Strip prefix") {
    assertEquals(schema.Person.stripPrefix(schema.base), "Person")
    assertEquals(xsd.integer.stripPrefix(xsd.base), "integer")
  }

  test("Append segment") {
    val expected = iri"http://senscience.ai/a/b"
    List(
      iri"http://senscience.ai/a"  -> "b",
      iri"http://senscience.ai/a/" -> "/b",
      iri"http://senscience.ai/a/" -> "b",
      iri"http://senscience.ai/a"  -> "/b"
    ).foreach { case (iri, segment) => assertEquals(iri / segment, expected) }
  }

  test("Extract its query parameters") {
    List(
      iri"http://senscience.ai?"            -> Query.blank,
      iri"http://senscience.ai?a=1&b=2&b=3" -> Query.fromPairs("a" -> "1", "b" -> "2", "b" -> "3"),
      iri"http://senscience.ai?a"           -> Query("a" -> None)
    ).foreach { case (iri, qp) => assertEquals(iri.query(), qp) }
  }

  test("Remove query param field") {
    List(
      iri"http://senscience.ai?"                                   -> iri"http://senscience.ai?",
      iri"http://senscience.ai?a=1&c=2"                            -> iri"http://senscience.ai?a=1&c=2",
      iri"http://senscience.ai?b=1&b=2"                            -> iri"http://senscience.ai",
      iri"http://senscience.ai?b=1&b=2&a=1"                        -> iri"http://senscience.ai?a=1",
      iri"http://user:pass@senscience.ai?d=1b&b&ab=1&b=2&b=3#frag" -> iri"http://user:pass@senscience.ai?ab=1#frag"
    ).foreach { case (iri, afterRemoval) =>
      assertEquals(iri.removeQueryParams("b", "d"), afterRemoval)
    }
  }

  test("Convert to Json") {
    assertEquals(iri.asJson, Json.fromString(iriString))
  }

  test("Construct from Json") {
    Json.fromString(iriString).as[Iri].assertRight(iri)
  }

  test("Get last path segment") {
    List(
      iri"http://senscience.ai/c?q=v",
      iri"http://senscience.ai/a/b/c#some",
      iri"http://senscience.ai/a/b/c/#some?q=a"
    ).foreach { iri =>
      assertEquals(iri.lastSegment, Some("c"))
    }

    List(iri"http://senscience.ai/", iri"http://senscience.ai//").foreach { iri =>
      assertEquals(iri.lastSegment, None)
    }
  }

  test("Order iris") {
    List(
      iri"http://example.com/a" -> iri"http://example.com/b",
      iri"http://example.com/a" -> iri"http://example.com/a/"
    ).foreach { case (first, second) =>
      assert(first < second)
    }
  }
}
