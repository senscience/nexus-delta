package ai.senscience.nexus.delta.plugins.storage.storages.model

import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import fs2.io.file.Path
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredCodec, deriveConfiguredEncoder}
import io.circe.syntax.*
import io.circe.{Codec, Encoder}

sealed trait StorageValue extends Product with Serializable {

  /**
    * @return
    *   name of the storage
    */
  def name: Option[String]

  /**
    * @return
    *   description of the storage
    */
  def description: Option[String]

  /**
    * @return
    *   the storage type
    */
  def tpe: StorageType

  /**
    * @return
    *   ''true'' if this store is the project's default, ''false'' otherwise
    */
  def default: Boolean

  /**
    * @return
    *   the digest algorithm, e.g. "SHA-256"
    */
  def algorithm: DigestAlgorithm

  /**
    * @return
    *   the maximum allowed file size (in bytes) for uploaded files
    */
  def maxFileSize: Long

  /**
    * @return
    *   the permission required in order to download a file to this storage
    */
  def readPermission: Permission

  /**
    * @return
    *   the permission required in order to upload a file to this storage
    */
  def writePermission: Permission
}

object StorageValue {

  /**
    * Resolved values to create/update a disk storage
    *
    * @see
    *   [[StorageFields.DiskStorageFields]]
    */
  final case class DiskStorageValue(
      name: Option[String] = None,
      description: Option[String] = None,
      default: Boolean,
      algorithm: DigestAlgorithm,
      volume: AbsolutePath,
      readPermission: Permission,
      writePermission: Permission,
      maxFileSize: Long
  ) extends StorageValue {

    override val tpe: StorageType = StorageType.DiskStorage

    def rootDirectory(project: ProjectRef): Path =
      Path.fromNioPath(volume.value.resolve(project.toString))
  }

  object DiskStorageValue {

    /**
      * @return
      *   a DiskStorageValue without name or description
      */
    def apply(
        default: Boolean,
        algorithm: DigestAlgorithm,
        volume: AbsolutePath,
        readPermission: Permission,
        writePermission: Permission,
        maxFileSize: Long
    ): DiskStorageValue =
      DiskStorageValue(None, None, default, algorithm, volume, readPermission, writePermission, maxFileSize)

  }

  /**
    * Resolved values to create/update a S3 compatible storage
    *
    * @see
    *   [[StorageFields.S3StorageFields]]
    */
  final case class S3StorageValue(
      name: Option[String],
      description: Option[String],
      default: Boolean,
      bucket: String,
      readPermission: Permission,
      writePermission: Permission,
      maxFileSize: Long,
      algorithm: DigestAlgorithm = DigestAlgorithm.default
  ) extends StorageValue {

    override val tpe: StorageType = StorageType.S3Storage
  }

  object S3StorageValue {

    /**
      * @return
      *   a S3StorageValue without name or description
      */
    def apply(
        default: Boolean,
        bucket: String,
        readPermission: Permission,
        writePermission: Permission,
        maxFileSize: Long
    ): S3StorageValue =
      S3StorageValue(
        None,
        None,
        default,
        bucket,
        readPermission,
        writePermission,
        maxFileSize
      )
  }

  implicit private[model] val storageValueEncoder: Encoder.AsObject[StorageValue] = {
    implicit val config: Configuration = Configuration.default.withDiscriminator(keywords.tpe)

    Encoder.encodeJsonObject.contramapObject { storage =>
      deriveConfiguredEncoder[StorageValue].encodeObject(storage).add(keywords.tpe, storage.tpe.iri.asJson)
    }
  }

  def databaseCodec(implicit configuration: Configuration): Codec.AsObject[StorageValue] =
    deriveConfiguredCodec[StorageValue]

}
