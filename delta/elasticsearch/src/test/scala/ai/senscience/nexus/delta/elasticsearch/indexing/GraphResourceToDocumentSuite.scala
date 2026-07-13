package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.rdf.Fixtures
import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.Triple.{obj, predicate}
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.testkit.mu.{JsonAssertions, NexusSuite}
import io.circe.syntax.KeyOps
import io.circe.{Json, JsonObject}

import java.time.Instant

class GraphResourceToDocumentSuite extends NexusSuite with Fixtures with JsonAssertions {

  private val entityType = EntityType("entityType")
  private val project    = ProjectRef.unsafe("org", "project")
  private val id         = iri"http://nexus.example.com/6A518B91-7B12-451B-8E85-48C67432C3A1"
  private val rev        = 1
  private val deprecated = false
  private val schema     = ResourceRef(iri"https://schema.org/Person")
  private val types      = Set(iri"https://schema.org/Person")

  private val expandedJson =
    json"""
      [
        {
          "@id": "http://nexus.example.com/6A518B91-7B12-451B-8E85-48C67432C3A1",
          "@type": "https://schema.org/Person",
          "https://schema.org/name": {"@value": "John Doe"}
        }
      ]
        """

  private val expanded      = ExpandedJsonLd.expanded(expandedJson).rightValue
  private val graph         = Graph(expanded).accepted
  private val metadataGraph = Graph.empty(id)

  private val context = ContextValue.fromFile("contexts/elasticsearch-indexing.json").accepted

  private val graphResourceToDocument = new GraphResourceToDocument(context, false)

  test("If the source has an expanded Iri `@id` it should be used") {
    val source =
      json"""
        {
          "@id": "http://nexus.example.com/john-doe",
          "@type": "https://schema.org/Person",
          "name": "John Doe"
        }
          """
    val elem   = elemFromSource(source)

    val expectedJson =
      json"""
        {
          "@id" : "http://nexus.example.com/john-doe",
          "@type" : "https://schema.org/Person",
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
          "@type": "https://schema.org/Person",
          "name": "John Doe"
        }
          """
    val elem   = elemFromSource(source)

    val expectedJson =
      json"""
        {
          "@id" : "nxv:JohnDoe",
          "@type" : "https://schema.org/Person",
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
          "@type": "https://schema.org/Person",
          "name": "John Doe"
        }
      """
    val elem   = elemFromSource(source)

    val expectedJson =
      json"""
        {
          "@id" : "http://nexus.example.com/6A518B91-7B12-451B-8E85-48C67432C3A1",
          "@type" : "https://schema.org/Person",
          "name" : "John Doe"
        }
      """

    for {
      json <- graphResourceToDocument(elem).accepted.toOption
    } yield json.equalsIgnoreArrayOrder(expectedJson)
  }

  test("System metadata fields are nested under _nexus") {
    val projectPredicate = iri"https://bluebrain.github.io/nexus/vocabulary/project"
    val metadataNode     = BNode.random
    val metadata         = Graph.empty(metadataNode).add(predicate(projectPredicate), obj("org/project"))
    val metadataContext  =
      context.merge(ContextValue.ContextObject(JsonObject("_project" := projectPredicate)))
    val pipe             = new GraphResourceToDocument(metadataContext, false)

    val source =
      json"""
        {
          "@id": "http://localhost/john-doe",
          "@type": "https://schema.org/Person",
          "name": "John Doe"
        }
          """

    val graphResource = GraphResource(
      entityType,
      project,
      id,
      rev,
      deprecated,
      schema,
      types,
      graph,
      metadata,
      source
    )
    val elem          = SuccessElem(entityType, id, project, Instant.EPOCH, Offset.start, graphResource, 1)

    val expectedJson =
      json"""
        {
          "@id" : "http://localhost/john-doe",
          "@type" : "https://schema.org/Person",
          "name" : "John Doe",
          "_nexus" : { "_project" : "org/project" }
        }
          """

    for {
      json <- pipe(elem).accepted.toOption
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
