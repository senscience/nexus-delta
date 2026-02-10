package ai.senscience.nexus.delta.sdk.organizations.model

import ai.senscience.nexus.delta.rdf.Vocabulary.*
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.generators.OrganizationGen
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.testkit.mu.NexusSuite

class OrganizationSuite extends NexusSuite with Fixtures {

  private val description  = "My description"
  private val organization = OrganizationGen.organization("myorg", description = Some(description))
  private val compacted    =
    json"""{"@context": "${contexts.organizations}", "_label": "${organization.label}", "_uuid": "${organization.uuid}", "description": "$description"}"""

  private val expanded =
    json"""[{"${nxv.label.iri}" : [{"@value" : "${organization.label}"} ], "${nxv.uuid.iri}" : [{"@value" : "${organization.uuid}"} ], "${nxv + "description"}" : [{"@value" : "$description"} ] } ]"""

  test("An Organization should be converted to Json-LD Compacted") {
    organization.toCompactedJsonLd.map(_.json).assertEquals(compacted)
  }

  test("An Organization should be converted to Json-LD expanded") {
    organization.toExpandedJsonLd.map(_.json).assertEquals(expanded)
  }
}
