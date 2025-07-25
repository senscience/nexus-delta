package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.Triple.{predicate, subject}
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.testkit.CirceLiteral
import org.scalatest.{Assertions, OptionValues}

trait Fixtures extends CirceLiteral {

  implicit val api: JsonLdApi = TitaniumJsonLdApi.strict

  val iri = iri"http://nexus.example.com/john-doé"

  // format: off
  val remoteContexts: Map[Iri, ContextValue] =
    Map(
      iri"http://example.com/cöntéxt/0"  -> ContextValue(json"""{"deprecated": {"@id": "http://schema.org/deprecated", "@type": "http://www.w3.org/2001/XMLSchema#boolean"} }"""),
      iri"http://example.com/cöntéxt/1"  -> ContextValue(json"""["http://example.com/cöntéxt/11", "http://example.com/cöntéxt/12"]"""),
      iri"http://example.com/cöntéxt/11" -> ContextValue(json"""{"birthDate": "http://schema.org/birthDate"}"""),
      iri"http://example.com/cöntéxt/12" -> ContextValue(json"""{"Other": "http://schema.org/Other"}"""),
      iri"http://example.com/cöntéxt/2"  -> ContextValue(json"""{"integerAlias": "http://www.w3.org/2001/XMLSchema#integer", "type": "@type"}"""),
      iri"http://example.com/cöntéxt/3"  -> ContextValue(json"""{"customid": {"@type": "@id"} }""")
    )
  // format: on

  implicit val remoteResolution: RemoteContextResolution = RemoteContextResolution.fixed(remoteContexts.toSeq*)

  object vocab {
    val value                  = iri"http://example.com/"
    def +(string: String): Iri = iri"$value$string"
  }

  object base {
    val value                  = iri"http://nexus.example.com/"
    def +(string: String): Iri = iri"$value$string"
  }
}

object Fixtures extends Fixtures

trait GraphHelpers extends OptionValues {
  self: Assertions =>

  def bNode(graph: Graph): BNode =
    BNode.unsafe(
      graph
        .find { case (s, p, _) => s == subject(graph.rootNode) && p == predicate(Fixtures.vocab + "address") }
        .map(_._3.getBlankNodeLabel)
        .value
    )
}
