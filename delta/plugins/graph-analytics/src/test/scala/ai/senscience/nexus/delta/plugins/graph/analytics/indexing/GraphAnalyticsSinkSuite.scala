package ai.senscience.nexus.delta.plugins.graph.analytics.indexing

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchClientSetup, NexusElasticsearchSuite}
import ai.senscience.nexus.delta.plugins.graph.analytics.indexing.GraphAnalyticsResult.Index
import ai.senscience.nexus.delta.plugins.graph.analytics.model.JsonLdDocument
import ai.senscience.nexus.delta.plugins.storage.files.nxvFile
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef
import ai.senscience.nexus.delta.sdk.resources.Resources
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.{FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.testkit.mu.JsonAssertions
import cats.effect.IO
import fs2.Chunk
import io.circe.Json
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.*

class GraphAnalyticsSinkSuite
    extends NexusElasticsearchSuite
    with ElasticSearchClientSetup.Fixture
    with JsonAssertions {

  override def munitFixtures: Seq[AnyFixture[?]] = List(esClient)

  private lazy val client = esClient()

  private val index = IndexLabel.unsafe("test_analytics")

  private lazy val sink = new GraphAnalyticsSink(client, BatchConfig(5, 100.millis), index)

  private val project = ProjectRef.unsafe("myorg", "myproject")

  private val remoteContexts: Set[RemoteContextRef] =
    Set(RemoteContextRef.StaticContextRef(iri"https://bluebrain.github.io/nexus/contexts/metadata.json"))

  // resource1 has references to 'resource3', 'file1' and 'generatedBy',
  // 'generatedBy' remains unresolved
  private val resource1 = iri"http://localhost/resource1"
  private val expanded1 = loadExpanded("expanded/resource1.json")

  // resource2 has references to other resources
  // All of them should remain unresolved
  private val resource2 = iri"http://localhost/resource2"
  private val expanded2 = loadExpanded("expanded/resource2.json")

  // Deprecated resource
  private val deprecatedResource      = iri"http://localhost/deprecated"
  private val deprecatedResourceTypes =
    Set(iri"https://schema.org/Dataset", iri"https://neuroshapes.org/NeuroMorphology")

  // Resource linked by 'resource1', resolved while indexing
  private val resource3 = iri"http://localhost/resource3"
  // File linked by 'resource1', resolved after an update by query
  private val file1     = iri"http://localhost/file1"

  private def loadExpanded(path: String): ExpandedJsonLd =
    loader
      .jsonContentOf(path)
      .flatMap { json =>
        IO.fromEither(ExpandedJsonLd.expanded(json))
      }
      .accepted

  private def getTypes(expandedJsonLd: ExpandedJsonLd): IO[Set[Iri]] =
    IO.pure(expandedJsonLd.cursor.getTypes.getOrElse(Set.empty))

  private val findRelationships: IO[Map[Iri, Set[Iri]]] = {
    for {
      resource1Types <- getTypes(expanded1)
      resource2Types <- getTypes(expanded2)
    } yield Map(
      resource1 -> resource1Types,
      resource2 -> resource2Types,
      resource3 -> Set(iri"https://neuroshapes.org/Trace")
    )
  }

  test("Create the update script and the index") {
    for {
      script   <- scriptContent
      _        <- client.createScript(updateRelationshipsScriptId, script)
      indexDef <- graphAnalyticsIndexDef
      _        <- client.createIndex(index, indexDef).assertEquals(true)
    } yield ()
  }

  private def success(id: Iri, result: GraphAnalyticsResult) =
    SuccessElem(Resources.entityType, id, project, Instant.EPOCH, Offset.start, result, 1)

  private def assertDocument(resourceId: Iri, expectedPath: String)(using Location) = {
    loader.jsonContentOf(expectedPath).flatMap { expected =>
      client.getSource[Json](index, resourceId.toString).map {
        case Some(result) =>
          result.equalsIgnoreArrayOrder(expected)
        case None         =>
          fail(s"Resource $resourceId should have been indexed")
      }
    }
  }

  test("Push index results") {
    def indexActive(id: Iri, expanded: ExpandedJsonLd) = {
      for {
        types <- getTypes(expanded)
        doc   <- JsonLdDocument.fromExpanded(expanded, _ => findRelationships)
      } yield {
        val result =
          Index.active(project, id, remoteContexts, 1, types, Instant.EPOCH, Anonymous, Instant.EPOCH, Anonymous, doc)
        success(id, result)
      }
    }

    def indexDeprecated(id: Iri, types: Set[Iri]) =
      success(
        id,
        Index.deprecated(project, id, remoteContexts, 1, types, Instant.EPOCH, Anonymous, Instant.EPOCH, Anonymous)
      )

    for {
      active1   <- indexActive(resource1, expanded1)
      active2   <- indexActive(resource2, expanded2)
      discarded  = success(resource3, GraphAnalyticsResult.Noop)
      deprecated = indexDeprecated(deprecatedResource, deprecatedResourceTypes)
      chunk      = Chunk(active1, active2, discarded, deprecated)
      // We expect no error
      _         <- sink(chunk).assertEquals(chunk.map(_.void))
      // 3 documents should have been indexed correctly:
      // - `resource1` with the relationship to `resource3` resolved
      // - `resource2` with no reference resolved
      // - `deprecatedResource` with only metadata, resolution is skipped
      _         <- client.refresh(index)
      _         <- client.count(index.value).assertEquals(3L)
      _         <- assertDocument(resource1, "result/resource1.json")
      _         <- assertDocument(resource2, "result/resource2.json")
      _         <- assertDocument(deprecatedResource, "result/resource_deprecated.json")
    } yield ()

  }

  test("Push update by query result results") {
    val error = new IllegalStateException("BOOM")
    val chunk = Chunk(
      success(file1, GraphAnalyticsResult.UpdateByQuery(file1, Set(nxvFile))),
      success(resource3, GraphAnalyticsResult.Noop),
      FailedElem(Resources.entityType, resource3, project, Instant.EPOCH, Offset.start, error, 1)
    )

    for {
      _ <- sink(chunk).assertEquals(chunk.map(_.void))
      // The reference to file1 should have been resolved and introduced as a relationship
      // The update query should not have an effect on the other resource
      _ <- client.refresh(index)
      _ <- client.count(index.value).assertEquals(3L)
      _ <- assertDocument(resource1, "result/resource1_updated.json")
      _ <- assertDocument(resource2, "result/resource2.json")
    } yield ()
  }

}
