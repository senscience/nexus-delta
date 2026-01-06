package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.configured.ConfiguredIndexDocument
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.generators.ResourceGen
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.testkit.mu.NexusSuite

import java.time.Instant

class AnnotatedSourceToConfiguredDocumentSuite extends NexusSuite {

  private given BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val personType = iri"https://schema.org/Person"
  private val roleType   = iri"https://schema.org/Role"

  private val entityType = EntityType("entity")
  private val project    = ProjectRef.unsafe("senscience", "my-project")
  private val id         = iri"https://senscience.ai/test/id"
  private val rev        = 1

  private val annotatedSourceToConfiguredDocument = new AnnotatedSourceToConfiguredDocument(Set(roleType))

  private def originalAnnotated(types: Set[Iri]) =
    SuccessElem[OriginalSource.Annotated](
      entityType,
      id,
      project,
      Instant.EPOCH,
      Offset.start,
      OriginalSource.Annotated(
        ResourceGen.resourceFUnit(id, project, types),
        json"""{ "@context" : "https://senscience.ai/nexus/contexts/metadata.json", "source": "xxx" }"""
      ),
      rev
    )

  test("Transform the annotated source in the expected document") {
    val input    = originalAnnotated(Set(roleType))
    val json     =
      json"""{
            "source": "xxx",
            "@id": "https://senscience.ai/test/id",
            "@type": "https://schema.org/Role",
            "_project": "senscience/my-project",
            "_rev": 1,
            "_deprecated": false,
            "_createdAt": "1970-01-01T00:00:00Z",
            "_createdBy": "http://localhost/v1/anonymous",
            "_updatedAt": "1970-01-01T00:00:00Z",
            "_updatedBy": "http://localhost/v1/anonymous",
            "_self": "http://localhost/v1/resources/senscience/my-project/_/https:%2F%2Fsenscience.ai%2Ftest%2Fid",
            "_constrainedBy": "https://bluebrain.github.io/nexus/schemas/unconstrained.json"
      }"""
    val expected = ConfiguredIndexDocument(Set(roleType), json)
    annotatedSourceToConfiguredDocument(input).map(_.toOption).assertEquals(Some(expected))
  }

  test("Filter out non-configured types") {
    val input    = originalAnnotated(Set(personType))
    val expected = input.dropped
    annotatedSourceToConfiguredDocument(input).assertEquals(expected)
  }

}
