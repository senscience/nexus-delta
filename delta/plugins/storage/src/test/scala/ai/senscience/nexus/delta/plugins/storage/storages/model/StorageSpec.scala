package ai.senscience.nexus.delta.plugins.storage.storages.model

import ai.senscience.nexus.delta.plugins.storage.RemoteContextResolutionFixture
import ai.senscience.nexus.delta.plugins.storage.storages.StorageFixtures
import ai.senscience.nexus.delta.plugins.storage.storages.model.Storage.*
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec

class StorageSpec extends CatsEffectSpec with RemoteContextResolutionFixture with StorageFixtures {

  "A Storage" should {
    val project     = ProjectRef(Label.unsafe("org"), Label.unsafe("project"))
    val diskStorage =
      DiskStorage(nxv + "disk", project, diskVal, json"""{"disk": "value"}""")
    val s3Storage   = S3Storage(nxv + "s3", project, s3Val, json"""{"s3": "value"}""")

    "be compacted" in {
      forAll(
        List(
          diskStorage -> diskJson.deepMerge(json"""{"@type": ["Storage", "DiskStorage"]}"""),
          s3Storage   -> s3Json
            .deepMerge(json"""{"@type": ["Storage", "S3Storage"]}""")
            .removeKeys("accessKey", "secretKey")
        )
      ) { case (value, compacted) =>
        value.toCompactedJsonLd.accepted.json shouldEqual compacted
      }
    }

    "be expanded" in {
      val diskJson = jsonContentOf("storages/disk-storage-expanded.json")
      val s3Json   = jsonContentOf("storages/s3-storage-expanded.json")

      forAll(List(diskStorage -> diskJson, s3Storage -> s3Json)) { case (value, expanded) =>
        value.toExpandedJsonLd.accepted.json shouldEqual expanded
      }
    }
  }

}
