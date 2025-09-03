package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.testkit.CirceLiteral

trait Fixtures extends CirceLiteral {

  implicit val api: JsonLdApi = TitaniumJsonLdApi.strict

  val iri = iri"http://nexus.senscience.ai/john-doé"

  // format: off
  val remoteContexts: Map[Iri, ContextValue] =
    Map(
      iri"http://senscience.ai/cöntéxt/0"  -> ContextValue(json"""{"deprecated": {"@id": "http://schema.org/deprecated", "@type": "http://www.w3.org/2001/XMLSchema#boolean"} }"""),
      iri"http://senscience.ai/cöntéxt/1"  -> ContextValue(json"""["http://senscience.ai/cöntéxt/11", "http://senscience.ai/cöntéxt/12"]"""),
      iri"http://senscience.ai/cöntéxt/11" -> ContextValue(json"""{"birthDate": "http://schema.org/birthDate"}"""),
      iri"http://senscience.ai/cöntéxt/12" -> ContextValue(json"""{"Other": "http://schema.org/Other"}"""),
      iri"http://senscience.ai/cöntéxt/2"  -> ContextValue(json"""{"integerAlias": "http://www.w3.org/2001/XMLSchema#integer", "type": "@type"}"""),
      iri"http://senscience.ai/cöntéxt/3"  -> ContextValue(json"""{"customid": {"@type": "@id"} }""")
    )
  // format: on

  implicit val remoteResolution: RemoteContextResolution = RemoteContextResolution.fixed(remoteContexts.toSeq*)

  object vocab {
    val value                  = iri"http://senscience.ai/"
    def +(string: String): Iri = iri"$value$string"
  }

  object base {
    val value                  = iri"http://nexus.senscience.ai/"
    def +(string: String): Iri = iri"$value$string"
  }
}

object Fixtures extends Fixtures

trait GraphHelpers {

  def addressBNode(graph: Graph): BNode =
    BNode.unsafe(
      graph
        .find(graph.rootNode, Fixtures.vocab + "address")
        .map(_.getBlankNodeLabel)
        .getOrElse(throw new IllegalAccessException("The BNode should be defined"))
    )

}
