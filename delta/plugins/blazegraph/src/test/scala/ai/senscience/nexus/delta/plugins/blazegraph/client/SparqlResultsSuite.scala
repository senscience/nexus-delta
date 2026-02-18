package ai.senscience.nexus.delta.plugins.blazegraph.client

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlResults.*
import ai.senscience.nexus.testkit.mu.{JsonAssertions, NexusSuite}
import io.circe.syntax.*
import org.http4s.syntax.literals.uri

class SparqlResultsSuite extends NexusSuite with JsonAssertions {

  private val json     = jsonContentOf("sparql/results/query-result.json")
  private val askJson  = jsonContentOf("sparql/results/ask-result.json")
  private val askJson2 = jsonContentOf("sparql/results/ask-result-2.json")

  private val blurb = Binding(
    "literal",
    "<p xmlns=\"http://www.w3.org/1999/xhtml\">My name is <b>alice</b></p>",
    None,
    Some("http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral")
  )

  private val map1 = Map(
    "x"      -> Binding("bnode", "r1"),
    "hpage"  -> Binding("uri", "http://work.example.org/alice/"),
    "name"   -> Binding("literal", "Alice"),
    "mbox"   -> Binding("literal", ""),
    "blurb"  -> blurb,
    "friend" -> Binding("bnode", "r2")
  )

  private val map2 = Map(
    "x"      -> Binding("bnode", "r2"),
    "hpage"  -> Binding("uri", "http://work.example.org/bob/"),
    "name"   -> Binding("literal", "Bob", Some("en")),
    "mbox"   -> Binding("uri", "mailto:bob@work.example.org"),
    "friend" -> Binding("bnode", "r1")
  )

  private val head = Head(
    List("x", "hpage", "name", "mbox", "age", "blurb", "friend"),
    Some(List(uri"http://www.w3.org/TR/rdf-sparql-XMLres/example.rq"))
  )

  private val qr    = SparqlResults(head, Bindings(map1, map2))
  private val qrAsk = SparqlResults(Head(), Bindings(), Some(true))

  test("A SparqlResults should be encoded") {
    qr.asJson.deepDropNullValues.equalsIgnoreArrayOrder(json)
    qrAsk.asJson.deepDropNullValues.equalsIgnoreArrayOrder(askJson2)
  }

  test("A SparqlResults should be decoded") {
    json.as[SparqlResults].assertRight(qr)
    askJson.as[SparqlResults].assertRight(qrAsk)
    askJson2.as[SparqlResults].assertRight(qrAsk)
  }

  test("Head should be merged") {
    assertEquals(
      head ++ Head(List("v", "hpage", "name")),
      Head(
        List("x", "hpage", "name", "mbox", "age", "blurb", "friend", "v"),
        Some(List(uri"http://www.w3.org/TR/rdf-sparql-XMLres/example.rq"))
      )
    )

    assertEquals(
      Head(List("v", "hpage", "name"), Some(List(uri"http://example.com/b"))) ++ Head(
        List("x", "hpage", "name"),
        Some(List(uri"http://example.com/a"))
      ),
      Head(List("v", "hpage", "name", "x"), Some(List(uri"http://example.com/b", uri"http://example.com/a")))
    )
  }

  test("Bindings should be merged") {
    assertEquals(Bindings(map1) ++ Bindings(map2), qr.results)
  }
}
