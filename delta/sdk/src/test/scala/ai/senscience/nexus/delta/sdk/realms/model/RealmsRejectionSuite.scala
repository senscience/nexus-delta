package ai.senscience.nexus.delta.sdk.realms.model

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.sdk.realms.model.RealmRejection.{IncorrectRev, RealmAlreadyExists}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*

class RealmsRejectionSuite extends NexusSuite with Fixtures {

  private val incorrectRev  = IncorrectRev(2, 3)
  private val alreadyExists = RealmAlreadyExists(Label.unsafe("name"))

  test("A RealmsRejection should be converted to compacted JSON-LD") {
    val list = List(
      alreadyExists -> json"""{"@type": "RealmAlreadyExists", "reason": "${alreadyExists.reason}"}""",
      incorrectRev  -> jsonContentOf("realms/incorrect-revision-compacted.json")
    )
    list.traverse { case (rejection, json) =>
      rejection.toCompactedJsonLd.map(_.json).assertEquals(json.addContext(contexts.error))
    }
  }

  test("A RealmsRejection should be converted to expanded JSON-LD") {
    val list = List(
      alreadyExists -> json"""[{"@type": ["${nxv + "RealmAlreadyExists"}"], "${nxv + "reason"}": [{"@value": "${alreadyExists.reason}"} ] } ]""",
      incorrectRev  -> jsonContentOf("realms/incorrect-revision-expanded.json")
    )
    list.traverse { case (rejection, json) =>
      rejection.toExpandedJsonLd.map(_.json).assertEquals(json)
    }
  }

}
