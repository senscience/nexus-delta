package ai.senscience.nexus.delta.plugins.storage.storages.model

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.storage.RemoteContextResolutionFixture
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageFields.*
import ai.senscience.nexus.delta.plugins.storage.storages.{contexts, StorageDecoderConfiguration, StorageFixtures}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.decoder.Configuration
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdSourceProcessor.JsonLdSourceDecoder
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec

class StorageFieldsSpec extends CatsEffectSpec with RemoteContextResolutionFixture with StorageFixtures {

  implicit private val cfg: Configuration = StorageDecoderConfiguration.apply.accepted
  val sourceDecoder                       = new JsonLdSourceDecoder[StorageFields](contexts.storages, UUIDF.random)

  "StorageFields" when {
    val pc = ProjectContext.unsafe(ApiMappings.empty, nxv.base, nxv.base, enforceSchema = false)

    "dealing with disk storages" should {
      val json = diskJson.addContext(contexts.storages)

      "be created from Json-LD" in {
        sourceDecoder(pc, json).accepted._2 shouldEqual diskFields
      }

      "be created from Json-LD without optional values" in {
        val jsonNoDefaults = json.removeKeys(
          "name",
          "description",
          "readPermission",
          "writePermission",
          "maxFileSize",
          "volume"
        )
        sourceDecoder(pc, jsonNoDefaults).accepted._2 shouldEqual
          DiskStorageFields(None, None, default = true, None, None, None, None)
      }
    }

    "dealing with S3 storages" should {
      val json = s3FieldsJson.addContext(contexts.storages)

      "be created from Json-LD" in {
        sourceDecoder(pc, json).accepted._2 shouldEqual s3Fields
      }

      "be created from Json-LD without optional values" in {
        val jsonNoDefaults =
          json.removeKeys(
            "name",
            "description",
            "readPermission",
            "writePermission",
            "maxFileSize",
            "endpoint",
            "accessKey",
            "secretKey",
            "region"
          )
        sourceDecoder(pc, jsonNoDefaults).accepted._2 shouldEqual
          S3StorageFields(None, None, default = true, Some("mybucket"), None, None, None)
      }
    }
  }

}
