package ai.senscience.nexus.delta.sdk.organizations.model

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec

class OrganizationRejectionsSpec extends CatsEffectSpec with Fixtures {

  "An OrganizationRejection" should {

    val incorrectRev  = IncorrectRev(2, 3)
    val alreadyExists = OrganizationAlreadyExists(Label.unsafe("org"))

    "be converted to compacted JSON-LD" in {
      val list = List(
        alreadyExists -> jsonContentOf("organizations/organization-already-exists-compacted.json"),
        incorrectRev  -> jsonContentOf("organizations/incorrect-revision-compacted.json")
      )
      forAll(list) { case (rejection, json) =>
        rejection.toCompactedJsonLd.accepted.json shouldEqual json.addContext(contexts.error)
      }
    }

    "be converted to expanded JSON-LD" in {
      val list = List(
        alreadyExists -> jsonContentOf("organizations/organization-already-exists-expanded.json"),
        incorrectRev  -> jsonContentOf("organizations/incorrect-revision-expanded.json")
      )
      forAll(list) { case (rejection, json) =>
        rejection.toExpandedJsonLd.accepted.json shouldEqual json
      }
    }
  }

}
