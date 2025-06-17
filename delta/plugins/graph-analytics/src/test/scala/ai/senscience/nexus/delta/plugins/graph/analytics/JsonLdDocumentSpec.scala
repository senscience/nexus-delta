package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.plugins.graph.analytics.model.JsonLdDocument
import ai.senscience.nexus.delta.plugins.storage.files.nxvFile
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.testkit.CirceEq
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.effect.IO
import io.circe.syntax.EncoderOps

class JsonLdDocumentSpec extends CatsEffectSpec with ContextFixtures with CirceEq {

  "A JsonLdDocument" should {
    implicit val jsonLdApi: JsonLdApi = TitaniumJsonLdApi.lenient
    val input                         = jsonContentOf("reconstructed-cell.json")
    val expanded                      = ExpandedJsonLd(input).accepted

    "be generated from expanded Json resource" in {
      val nodeRef1                                  = iri"http://api.brain-map.org/api/v2/data/Structure/733"
      val nodeRef2                                  = iri"http://localhost/nexus/v1/files/my-file"
      def findRelationships: IO[Map[Iri, Set[Iri]]] = IO.pure(
        Map(
          nodeRef1 -> Set(iri"https://neuroshapes.org/NeuronMorphology"),
          nodeRef2 -> Set(nxvFile)
        )
      )
      val document                                  = JsonLdDocument.fromExpanded(expanded, _ => findRelationships)
      document.accepted.asJson should equalIgnoreArrayOrder(jsonContentOf("reconstructed-cell-document.json"))
    }
  }

}
