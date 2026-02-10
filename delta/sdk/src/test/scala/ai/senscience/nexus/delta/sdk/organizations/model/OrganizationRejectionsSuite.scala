package ai.senscience.nexus.delta.sdk.organizations.model

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*

class OrganizationRejectionsSuite extends NexusSuite with Fixtures {

  private val incorrectRev  = IncorrectRev(2, 3)
  private val alreadyExists = OrganizationAlreadyExists(Label.unsafe("org"))

  test("An OrganizationRejection should be converted to compacted JSON-LD") {
    val list = List(
      alreadyExists -> jsonContentOf("organizations/organization-already-exists-compacted.json"),
      incorrectRev  -> jsonContentOf("organizations/incorrect-revision-compacted.json")
    )
    list.traverse { case (rejection, json) =>
      rejection.toCompactedJsonLd.map(_.json).assertEquals(json.addContext(contexts.error))
    }
  }

  test("An OrganizationRejection should be converted to expanded JSON-LD") {
    val list = List(
      alreadyExists -> jsonContentOf("organizations/organization-already-exists-expanded.json"),
      incorrectRev  -> jsonContentOf("organizations/incorrect-revision-expanded.json")
    )
    list.traverse { case (rejection, json) =>
      rejection.toExpandedJsonLd.map(_.json).assertEquals(json)
    }
  }

}
