package ai.senscience.nexus.delta.sdk.permissions.model

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsRejection.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*

class PermissionsRejectionSuite extends NexusSuite with Fixtures {

  private val incorrectRev  = IncorrectRev(2, 3)
  private val cannotReplace = CannotReplaceWithEmptyCollection

  test("A PermissionsRejection should be converted to compacted JSON-LD") {
    val list = List(
      cannotReplace -> json"""{"@type": "CannotReplaceWithEmptyCollection", "reason": "${cannotReplace.reason}"}""",
      incorrectRev  -> jsonContentOf("permissions/incorrect-revision-compacted.json")
    )
    list.traverse { case (rejection, json) =>
      rejection.toCompactedJsonLd.map(_.json).assertEquals(json.addContext(contexts.error))
    }
  }

  test("A PermissionsRejection should be converted to expanded JSON-LD") {
    val list = List(
      cannotReplace -> json"""[{"@type": ["${nxv + "CannotReplaceWithEmptyCollection"}"], "${nxv + "reason"}": [{"@value": "${cannotReplace.reason}"} ] } ]""",
      incorrectRev  -> jsonContentOf("permissions/incorrect-revision-expanded.json")
    )
    list.traverse { case (rejection, json) =>
      rejection.toExpandedJsonLd.map(_.json).assertEquals(json)
    }
  }

}
