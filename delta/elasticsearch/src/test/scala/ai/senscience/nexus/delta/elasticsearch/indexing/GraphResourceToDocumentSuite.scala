package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.rdf.Fixtures
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.testkit.mu.{JsonAssertions, NexusSuite}
import io.circe.Json

import java.time.Instant

class GraphResourceToDocumentSuite extends NexusSuite with Fixtures with JsonAssertions {

  private val entityType = EntityType("entityType")
  private val project    = ProjectRef.unsafe("org", "project")
  private val id         = iri"http://nexus.example.com/6A518B91-7B12-451B-8E85-48C67432C3A1"
  private val rev        = 1
  private val deprecated = false
  private val schema     = ResourceRef(iri"http://schema.org/Person")
  private val types      = Set(iri"http://schema.org/Resource")

  private val expandedJson =
    json"""
      [
        {
          "@id": "http://nexus.example.com/6A518B91-7B12-451B-8E85-48C67432C3A1",
          "@type": "http://schema.org/Person",
          "http://schema.org/name": {"@value": "John Doe"}
        }
      ]
        """

  private val expanded      = ExpandedJsonLd.expanded(expandedJson).rightValue
  private val graph         = Graph(expanded).accepted
  private val metadataGraph = graph

  private val context = ContextValue.fromFile("contexts/elasticsearch-indexing.json").accepted

  private val graphResourceToDocument = new GraphResourceToDocument(context, false)

  test("If the source has an expanded Iri `@id` it should be used") {
    val source =
      json"""
        {
          "@id": "http://nexus.example.com/john-doe",
          "@type": "http://schema.org/Person",
          "name": "John Doe"
        }
          """
    val elem   = elemFromSource(source)

    val expectedJson =
      json"""
        {
          "@id" : "http://nexus.example.com/john-doe",
          "@type" : "http://schema.org/Person",
          "name" : "John Doe"
        }
          """

    for {
      json <- graphResourceToDocument(elem).accepted.toOption
    } yield json.equalsIgnoreArrayOrder(expectedJson)
  }

  test("If the source has a compacted Iri `@id` it should be used") {
    val source =
      json"""
        {
          "@id": "nxv:JohnDoe",
          "@type": "http://schema.org/Person",
          "name": "John Doe"
        }
          """
    val elem   = elemFromSource(source)

    val expectedJson =
      json"""
        {
          "@id" : "nxv:JohnDoe",
          "@type" : "http://schema.org/Person",
          "name" : "John Doe"
        }
          """

    for {
      json <- graphResourceToDocument(elem).accepted.toOption
    } yield json.equalsIgnoreArrayOrder(expectedJson)
  }

  test("If the source does not have an `@id` it should be injected") {
    val source =
      json"""
        {
          "@type": "http://schema.org/Person",
          "name": "John Doe"
        }
      """
    val elem   = elemFromSource(source)

    val expectedJson =
      json"""
        {
          "@id" : "http://nexus.example.com/6A518B91-7B12-451B-8E85-48C67432C3A1",
          "@type" : "http://schema.org/Person",
          "name" : "John Doe"
        }
      """

    for {
      json <- graphResourceToDocument(elem).accepted.toOption
    } yield json.equalsIgnoreArrayOrder(expectedJson)
  }

  private def elemFromSource(source: Json): SuccessElem[GraphResource] = {
    val graphResource = GraphResource(
      entityType,
      project,
      id,
      rev,
      deprecated,
      schema,
      types,
      graph,
      metadataGraph,
      source
    )
    SuccessElem(entityType, id, project, Instant.EPOCH, Offset.start, graphResource, 1)
  }

}
