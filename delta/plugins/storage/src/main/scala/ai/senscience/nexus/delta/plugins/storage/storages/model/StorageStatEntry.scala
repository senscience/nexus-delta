package ai.senscience.nexus.delta.plugins.storage.storages.model

import ai.senscience.nexus.delta.plugins.storage.storages.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Decoder, Encoder}

/**
  * The stats for a single storage
  *
  * @param files
  *   the number of physical files for this storage
  * @param spaceUsed
  *   the space used by the files for this storage
  */
final case class StorageStatEntry(files: Long, spaceUsed: Long)

object StorageStatEntry {

  given Encoder[StorageStatEntry] = deriveCodec[StorageStatEntry]

  given JsonLdEncoder[StorageStatEntry] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.storages))

  given Decoder[StorageStatEntry] =
    Decoder.instance { hc =>
      val aggregations = hc.downField("aggregations")
      for {
        size  <- aggregations.downField("storageSize").get[Long]("value")
        files <- aggregations.downField("filesCount").get[Long]("value")
      } yield StorageStatEntry(files, size)
    }

  given HttpResponseFields[StorageStatEntry] = HttpResponseFields.defaultOk

}
