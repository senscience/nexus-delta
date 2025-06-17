package ai.senscience.nexus.delta.sdk.organizations.model

import ai.senscience.nexus.delta.rdf.Vocabulary.*
import ai.senscience.nexus.delta.rdf.syntax.jsonLdEncoderSyntax
import ai.senscience.nexus.delta.sdk.generators.OrganizationGen
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec

class OrganizationSpec extends CatsEffectSpec with CirceLiteral with Fixtures {

  "An Organization" should {

    val organization = OrganizationGen.organization("myorg", description = Some("My description"))
    val compacted    =
      json"""{"@context": "${contexts.organizations}", "_label": "${organization.label}", "_uuid": "${organization.uuid}", "description": "${organization.description.value}"}"""

    val expanded =
      json"""[{"${nxv.label.iri}" : [{"@value" : "${organization.label}"} ], "${nxv.uuid.iri}" : [{"@value" : "${organization.uuid}"} ], "${nxv + "description"}" : [{"@value" : "${organization.description.value}"} ] } ]"""

    "be converted to Json-LD Compacted" in {
      organization.toCompactedJsonLd.accepted.json shouldEqual compacted
    }

    "be converted to Json-LD expanded" in {
      organization.toExpandedJsonLd.accepted.json shouldEqual expanded
    }
  }
}
