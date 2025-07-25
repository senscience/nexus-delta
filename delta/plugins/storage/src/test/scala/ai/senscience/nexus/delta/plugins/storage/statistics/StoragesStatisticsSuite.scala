package ai.senscience.nexus.delta.plugins.storage.statistics

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.elasticsearch.metrics.{EventMetrics, EventMetricsIndex}
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchClientSetup, Fixtures, NexusElasticsearchSuite}
import ai.senscience.nexus.delta.plugins.storage.files.nxvFile
import ai.senscience.nexus.delta.plugins.storage.statistics.StoragesStatisticsSuite.*
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesStatistics
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageStatEntry
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sdk.model.metrics.EventMetric.{Created, Deprecated, ProjectScopedMetric, TagDeleted, Tagged, Updated}
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import cats.effect.IO
import io.circe.JsonObject
import io.circe.syntax.KeyOps
import munit.AnyFixture

import java.time.Instant

class StoragesStatisticsSuite
    extends NexusElasticsearchSuite
    with ElasticSearchClientSetup.Fixture
    with EventMetricsIndex.Fixture
    with Fixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(esClient, metricsIndex)

  private lazy val client        = esClient()
  private lazy val mIndex        = metricsIndex()
  private lazy val eventsMetrics = EventMetrics(client, mIndex)

  private def stats = (client: ElasticSearchClient) =>
    StoragesStatistics.apply(client, (storage, _) => IO.pure(Iri.unsafe(storage.toString)), mIndex.name)

  test("Initialize the event metrics index and refresh it") {
    eventsMetrics.init >>
      eventsMetrics.index(Vector(metric1, metric2, metric3, metric4, metric5, metric6)) >>
      client.refresh(mIndex.name)
  }

  test("Correct statistics for storage in project 1") {
    stats(client).get("storageId", projectRef1).assertEquals(StorageStatEntry(2L, 30L))
  }

  test("Correct statistics for storage in project 2") {
    stats(client).get("storageId", projectRef2).assertEquals(StorageStatEntry(1L, 20L))
  }

  test("Zero stats for non-existing storage") {
    stats(client).get("none", projectRef1).assertEquals(StorageStatEntry(0L, 0L))
  }

}

object StoragesStatisticsSuite {
  private val org             = Label.unsafe("org")
  private val proj1           = Label.unsafe("proj1")
  private val proj2           = Label.unsafe("proj2")
  val projectRef1: ProjectRef = ProjectRef(org, proj1)
  val projectRef2: ProjectRef = ProjectRef(org, proj2)

  private val metric1 = ProjectScopedMetric(
    Instant.EPOCH,
    Anonymous,
    1,
    Set(Created),
    projectRef1,
    iri"http://bbp.epfl.ch/file1",
    Set(nxvFile),
    JsonObject(
      "storage"        := "storageId",
      "newFileWritten" := 1,
      "bytes"          := 10L
    )
  )

  private val metric2 = ProjectScopedMetric(
    Instant.EPOCH,
    Anonymous,
    2,
    Set(Updated),
    projectRef1,
    iri"http://bbp.epfl.ch/file1",
    Set(nxvFile),
    JsonObject(
      "storage"        := "storageId",
      "newFileWritten" := 1,
      "bytes"          := 20L
    )
  )

  private val metric3 = ProjectScopedMetric(
    Instant.EPOCH,
    Anonymous,
    3,
    Set(Tagged),
    projectRef1,
    iri"http://bbp.epfl.ch/file1",
    Set(nxvFile),
    JsonObject("storage" := "storageId")
  )

  private val metric4 = ProjectScopedMetric(
    Instant.EPOCH,
    Anonymous,
    4,
    Set(TagDeleted),
    projectRef1,
    iri"http://bbp.epfl.ch/file1",
    Set(nxvFile),
    JsonObject("storage" := "storageId")
  )

  private val metric5 = ProjectScopedMetric(
    Instant.EPOCH,
    Anonymous,
    1,
    Set(Created),
    projectRef2,
    iri"http://bbp.epfl.ch/file2",
    Set(nxvFile),
    JsonObject(
      "storage"        := "storageId",
      "newFileWritten" := 1,
      "bytes"          := 20L
    )
  )

  private val metric6 = ProjectScopedMetric(
    Instant.EPOCH,
    Anonymous,
    2,
    Set(Deprecated),
    projectRef2,
    iri"http://bbp.epfl.ch/file2",
    Set(nxvFile),
    JsonObject("storage" := "storageId")
  )
}
