package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.generators.{PermissionsGen, ResourceGen}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.acls
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.{NexusSuite, TextAssertions}

class ResourceFSuite extends NexusSuite with Fixtures with TextAssertions {

  private given BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val updatedBy = User("maria", Label.unsafe("bbp"))
  private val resource  = PermissionsGen.resourceFor(Set(acls.read, acls.write), rev = 1, updatedBy = updatedBy)

  test("A ResourceF of a permission should be converted to Json-LD compacted") {
    resource.toCompactedJsonLd.map(_.json).assertEquals(jsonContentOf("resource-compacted.jsonld"))
  }

  test("A ResourceF of a permission should be converted to Json-LD expanded") {
    resource.toExpandedJsonLd.map(_.json).assertEquals(jsonContentOf("resource-expanded.jsonld"))
  }

  test("A ResourceF of a permission should be converted to Dot format") {
    resource.toDot.map { dot =>
      dot.toString.equalLinesUnordered(contentOf("resource-dot.dot"))
    }
  }

  test("A ResourceF of a permission should be converted to NTriples format") {
    resource.toNTriples.map { ntriples =>
      ntriples.toString.equalLinesUnordered(contentOf("resource-ntriples.nt"))
    }
  }

  test("A ResourceF of a data resource should be converted to Json-LD compacted") {
    val resourceF = ResourceGen.resourceFor(
      ResourceGen.resource(
        nxv + "testId",
        ProjectRef.unsafe("org", "proj"),
        jsonContentOf("resources/resource-with-context.json")
      ),
      Set(nxv + "TestResource")
    )
    resourceF.toCompactedJsonLd
      .map(_.json)
      .assertEquals(jsonContentOf("resources/resource-with-context-and-metadata.json"))
  }

}
