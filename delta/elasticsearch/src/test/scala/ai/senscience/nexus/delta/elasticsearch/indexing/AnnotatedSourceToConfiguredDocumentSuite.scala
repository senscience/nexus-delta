package ai.senscience.nexus.delta.elasticsearch.indexing

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
        json"""{ "source": "xxx" }"""
      ),
      rev
    )

  test("Transform the annotated source in the expected document") {
    val input = originalAnnotated(Set(roleType))
    annotatedSourceToConfiguredDocument(input).assert(
      _.toOption.isDefined,
      "A document should be created"
    )
  }

  test("Filter out non-configured types") {
    val input    = originalAnnotated(Set(personType))
    val expected = input.dropped
    annotatedSourceToConfiguredDocument(input).assertEquals(expected)
  }

}
