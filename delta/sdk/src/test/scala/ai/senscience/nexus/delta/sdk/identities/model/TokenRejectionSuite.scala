package ai.senscience.nexus.delta.sdk.identities.model

import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.*
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.sdk.error.IdentityError.given
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.testkit.mu.NexusSuite

class TokenRejectionSuite extends NexusSuite with Fixtures {

  private val invalidFormat = InvalidAccessTokenFormat("Details")
  private val noIssuer      = AccessTokenDoesNotContainSubject

  test("A TokenRejection should be converted to compacted JSON-LD") {
    val list = List(
      noIssuer      -> json"""{"@type": "AccessTokenDoesNotContainSubject", "reason": "${noIssuer.getMessage}"}""",
      invalidFormat -> json"""{"@type": "InvalidAccessTokenFormat", "reason": "${invalidFormat.getMessage}"}"""
    )
    list.foreach { case (rejection, expectedJson) =>
      rejection.toCompactedJsonLd.map(_.json).assertEquals(expectedJson.addContext(contexts.error))
    }
  }

  test("A TokenRejection should be converted to expanded JSON-LD") {
    val list = List(
      noIssuer      -> json"""[{"@type": ["${nxv + "AccessTokenDoesNotContainSubject"}"], "${nxv + "reason"}": [{"@value": "${noIssuer.getMessage}"} ] } ]""",
      invalidFormat -> json"""[{"@type": ["${nxv + "InvalidAccessTokenFormat"}"], "${nxv + "reason"}": [{"@value": "${invalidFormat.getMessage}"} ] } ]"""
    )
    list.foreach { case (rejection, expectedJson) =>
      rejection.toExpandedJsonLd.map(_.json).assertEquals(expectedJson)
    }
  }

}
