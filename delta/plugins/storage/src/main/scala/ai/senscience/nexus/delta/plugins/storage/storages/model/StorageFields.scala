package ai.senscience.nexus.delta.plugins.storage.storages.model

import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.StorageTypeConfig
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageValue.{DiskStorageValue, S3StorageValue}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.decoder.configuration.semiauto.deriveConfigJsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.{Configuration as JsonLdConfiguration, JsonLdDecoder}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder
import io.circe.syntax.*
import io.circe.{Encoder, Json}

sealed trait StorageFields extends Product with Serializable { self =>

  type Value <: StorageValue

  /**
    * @return
    *   the name of the storage
    */
  def name: Option[String]

  /**
    * @return
    *   the description of the storage
    */
  def description: Option[String]

  /**
    * @return
    *   the storage type
    */
  def tpe: StorageType

  /**
    * @return
    *   the maximum allowed file size (in bytes) for uploaded files
    */
  def maxFileSize: Option[Long]

  /**
    * @return
    *   the permission required in order to download a file to this storage
    */
  def readPermission: Option[Permission]

  /**
    * @return
    *   the permission required in order to upload a file to this storage
    */
  def writePermission: Option[Permission]

  /**
    * Converts the current [[StorageFields]] to a [[StorageValue]] resolving some optional values with the passed config
    */
  def toValue(config: StorageTypeConfig): Option[Value]

  /**
    * Returns the decrypted Json representation of the storage fields with the passed @id
    */
  def toJson(iri: Iri): Json =
    self.asJsonObject.add(keywords.id, iri.asJson).asJson
}

object StorageFields {

  private def computeMaxFileSize(payloadSize: Option[Long], configMaxFileSize: Long) =
    payloadSize.fold(configMaxFileSize)(size => Math.min(configMaxFileSize, size))

  /**
    * Necessary values to create/update a disk storage
    *
    * @param default
    *   ''true'' if this store is the project's default, ''false'' otherwise
    * @param volume
    *   the volume this storage is going to use to save files
    * @param readPermission
    *   the permission required in order to download a file from this storage
    * @param writePermission
    *   the permission required in order to upload a file to this storage
    * @param maxFileSize
    *   the maximum allowed file size (in bytes) for uploaded files
    */
  final case class DiskStorageFields(
      name: Option[String],
      description: Option[String],
      default: Boolean,
      volume: Option[AbsolutePath],
      readPermission: Option[Permission],
      writePermission: Option[Permission],
      maxFileSize: Option[Long]
  ) extends StorageFields {
    override val tpe: StorageType = StorageType.DiskStorage

    override type Value = DiskStorageValue

    override def toValue(config: StorageTypeConfig): Option[Value] =
      Some(
        DiskStorageValue(
          name,
          description,
          default,
          config.disk.digestAlgorithm,
          volume.getOrElse(config.disk.defaultVolume),
          readPermission.getOrElse(config.disk.defaultReadPermission),
          writePermission.getOrElse(config.disk.defaultWritePermission),
          computeMaxFileSize(maxFileSize, config.disk.defaultMaxFileSize)
        )
      )
  }

  /**
    * Necessary values to create/update a S3 compatible storage
    *
    * @param default
    *   ''true'' if this store is the project's default, ''false'' otherwise
    * @param bucket
    *   the S3 compatible bucket
    * @param readPermission
    *   the permission required in order to download a file from this storage
    * @param writePermission
    *   the permission required in order to upload a file to this storage
    * @param maxFileSize
    *   the maximum allowed file size (in bytes) for uploaded files
    */
  final case class S3StorageFields(
      name: Option[String],
      description: Option[String],
      default: Boolean,
      bucket: Option[String],
      readPermission: Option[Permission],
      writePermission: Option[Permission],
      maxFileSize: Option[Long]
  ) extends StorageFields {
    override val tpe: StorageType = StorageType.S3Storage

    override type Value = S3StorageValue

    override def toValue(config: StorageTypeConfig): Option[Value] =
      config.amazon.map { cfg =>
        S3StorageValue(
          name,
          description,
          default,
          bucket.getOrElse(cfg.defaultBucket),
          readPermission.getOrElse(cfg.defaultReadPermission),
          writePermission.getOrElse(cfg.defaultWritePermission),
          computeMaxFileSize(maxFileSize, cfg.defaultMaxFileSize)
        )
      }
  }

  implicit private[model] val storageFieldsEncoder: Encoder.AsObject[StorageFields] = {
    implicit val config: Configuration = Configuration.default.withDiscriminator(keywords.tpe)

    Encoder.encodeJsonObject.contramapObject { storage =>
      deriveConfiguredEncoder[StorageFields].encodeObject(storage).add(keywords.tpe, storage.tpe.iri.asJson)
    }
  }

  implicit def storageFieldsJsonLdDecoder(implicit cfg: JsonLdConfiguration): JsonLdDecoder[StorageFields] =
    deriveConfigJsonLdDecoder[StorageFields]
}
