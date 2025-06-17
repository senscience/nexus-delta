package ai.senscience.nexus.delta.plugins.storage.statistics

import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageStatEntry
import ai.senscience.nexus.testkit.mu.NexusSuite

class StorageStatisticsSerializationSuite extends NexusSuite {

  test("Statistics responses are deserialized correctly") {
    val json     = jsonContentOf("storages/statistics/single-storage-stats-response.json")
    val expected = StorageStatEntry(1, 1199813)

    for {
      stats <- json.as[StorageStatEntry]
      _      = assertEquals(stats, expected)
    } yield ()
  }

}
