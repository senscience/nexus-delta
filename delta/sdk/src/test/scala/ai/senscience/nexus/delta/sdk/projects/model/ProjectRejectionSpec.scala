package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec

class ProjectRejectionSpec extends CatsEffectSpec with Fixtures {

  "A ProjectRejection" should {
    val incorrectRev  = IncorrectRev(2, 3)
    val alreadyExists = ProjectAlreadyExists(ProjectRef(Label.unsafe("org"), Label.unsafe("proj")))

    "be converted to compacted JSON-LD" in {
      val list = List(
        alreadyExists -> jsonContentOf("projects/project-already-exists-compacted.json"),
        incorrectRev  -> jsonContentOf("projects/incorrect-revision-compacted.json")
      )
      forAll(list) { case (rejection, json) =>
        rejection.toCompactedJsonLd.accepted.json shouldEqual json.addContext(contexts.error)
      }
    }

    "be converted to expanded JSON-LD" in {
      val list = List(
        alreadyExists -> jsonContentOf("projects/project-already-exists-expanded.json"),
        incorrectRev  -> jsonContentOf("projects/incorrect-revision-expanded.json")
      )
      forAll(list) { case (rejection, json) =>
        rejection.toExpandedJsonLd.accepted.json shouldEqual json
      }
    }
  }

}
