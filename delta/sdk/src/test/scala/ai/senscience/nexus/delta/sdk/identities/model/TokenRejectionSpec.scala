package ai.senscience.nexus.delta.sdk.identities.model

import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.*
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.sdk.error.IdentityError.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec

class TokenRejectionSpec extends CatsEffectSpec with CirceLiteral with Fixtures {

  "A TokenRejection" should {

    val invalidFormat = InvalidAccessTokenFormat("Details")
    val noIssuer      = AccessTokenDoesNotContainSubject

    "be converted to compacted JSON-LD" in {
      val list = List(
        noIssuer      -> json"""{"@type": "AccessTokenDoesNotContainSubject", "reason": "${noIssuer.getMessage}"}""",
        invalidFormat -> json"""{"@type": "InvalidAccessTokenFormat", "reason": "${invalidFormat.getMessage}"}"""
      )
      forAll(list) { case (rejection, json) =>
        rejection.toCompactedJsonLd.accepted.json shouldEqual json.addContext(contexts.error)
      }
    }

    "be converted to expanded JSON-LD" in {
      val list = List(
        noIssuer      -> json"""[{"@type": ["${nxv + "AccessTokenDoesNotContainSubject"}"], "${nxv + "reason"}": [{"@value": "${noIssuer.getMessage}"} ] } ]""",
        invalidFormat -> json"""[{"@type": ["${nxv + "InvalidAccessTokenFormat"}"], "${nxv + "reason"}": [{"@value": "${invalidFormat.getMessage}"} ] } ]"""
      )
      forAll(list) { case (rejection, json) =>
        rejection.toExpandedJsonLd.accepted.json shouldEqual json
      }
    }
  }

}
