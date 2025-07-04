package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.kernel.Secret
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.StorageTypeConfig
import ai.senscience.nexus.delta.plugins.storage.storages.model.{AbsolutePath, DigestAlgorithm, StorageType}
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import cats.implicits.*
import org.http4s.Uri
import org.http4s.Uri.Path
import pureconfig.ConvertHelpers.optF
import pureconfig.error.{CannotConvert, ConfigReaderFailures, ConvertFailure, FailureReason}
import pureconfig.generic.auto.*
import pureconfig.module.http4s.*
import pureconfig.{ConfigConvert, ConfigReader}

/**
  * Configuration for the Storages module.
  *
  * @param eventLog
  *   configuration of the event log
  * @param pagination
  *   configuration for how pagination should behave in listing operations
  * @param storageTypeConfig
  *   configuration of each of the storage types
  */
final case class StoragesConfig(
    eventLog: EventLogConfig,
    pagination: PaginationConfig,
    storageTypeConfig: StorageTypeConfig
)

object StoragesConfig {

  implicit val storageConfigReader: ConfigReader[StoragesConfig] =
    ConfigReader.fromCursor { cursor =>
      for {
        obj              <- cursor.asObjectCursor
        eventLogCursor   <- obj.atKey("event-log")
        eventLog         <- ConfigReader[EventLogConfig].from(eventLogCursor)
        paginationCursor <- obj.atKey("pagination")
        pagination       <- ConfigReader[PaginationConfig].from(paginationCursor)
        storageType      <- ConfigReader[StorageTypeConfig].from(cursor)
      } yield StoragesConfig(eventLog, pagination, storageType)
    }

  /**
    * The configuration of each of the storage types
    *
    * @param disk
    *   configuration for the disk storage
    * @param amazon
    *   configuration for the s3 compatible storage
    */
  final case class StorageTypeConfig(
      disk: DiskStorageConfig,
      amazon: Option[S3StorageConfig]
  ) {
    def showFileLocation: ShowFileLocation = {
      val diskType = if (disk.showLocation) Set(StorageType.DiskStorage) else Set.empty
      val s3Type   = if (amazon.exists(_.showLocation)) Set(StorageType.S3Storage) else Set.empty
      ShowFileLocation(diskType ++ s3Type)
    }
  }

  final case class ShowFileLocation(types: Set[StorageType])

  object StorageTypeConfig {

    final private case class WrongAllowedKeys(defaultVolume: AbsolutePath) extends FailureReason {
      val description: String = s"'allowed-volumes' must contain at least '$defaultVolume' (default-volume)"
    }

    implicit val storageTypeConfigReader: ConfigReader[StorageTypeConfig] = ConfigReader.fromCursor { cursor =>
      for {
        obj                 <- cursor.asObjectCursor
        diskCursor          <- obj.atKey("disk")
        disk                <- ConfigReader[DiskStorageConfig].from(diskCursor)
        _                   <- Either.raiseUnless(disk.allowedVolumes.contains(disk.defaultVolume))(
                                 ConfigReaderFailures(ConvertFailure(WrongAllowedKeys(disk.defaultVolume), None, "disk.allowed-volumes"))
                               )
        amazonCursor        <- obj.atKeyOrUndefined("amazon").asObjectCursor
        amazonEnabledCursor <- amazonCursor.atKey("enabled")
        amazonEnabled       <- amazonEnabledCursor.asBoolean
        amazon              <- ConfigReader[S3StorageConfig].from(amazonCursor)
      } yield StorageTypeConfig(
        disk,
        Option.when(amazonEnabled)(amazon)
      )
    }

  }

  /**
    * Common parameters on different storages configuration
    */
  sealed trait StorageTypeEntryConfig {
    def defaultReadPermission: Permission
    def defaultWritePermission: Permission
    def showLocation: Boolean
    def defaultMaxFileSize: Long
  }

  /**
    * Disk storage configuration
    *
    * @param defaultVolume
    *   the base [[Path]] where the files are stored
    * @param allowedVolumes
    *   the allowed set of [[Path]] s where the files are stored
    * @param digestAlgorithm
    *   algorithm for checksum calculation
    * @param defaultReadPermission
    *   the default permission required in order to download a file from a disk storage
    * @param defaultWritePermission
    *   the default permission required in order to upload a file to a disk storage
    * @param showLocation
    *   flag to decide whether or not to show the absolute location of the files in the metadata response
    * @param defaultMaxFileSize
    *   the default maximum allowed file size (in bytes) for uploaded files
    */
  final case class DiskStorageConfig(
      defaultVolume: AbsolutePath,
      allowedVolumes: Set[AbsolutePath],
      digestAlgorithm: DigestAlgorithm,
      defaultReadPermission: Permission,
      defaultWritePermission: Permission,
      showLocation: Boolean,
      defaultMaxFileSize: Long
  ) extends StorageTypeEntryConfig

  /**
    * Amazon S3 compatible storage configuration
    *
    * @param defaultEndpoint
    *   the default endpoint of the current storage
    * @param useDefaultCredentialProvider
    *   indicates that the default AWS credential provider will be used. If this is specified, the access and secret
    *   keys will not be used, even if specified. Instead, this relies on delta running in an environment that is
    *   configured (with policies, roles) to allow access to S3.
    * @param defaultAccessKey
    *   the access key for the default endpoint, when provided
    * @param defaultSecretKey
    *   the secret key for the default endpoint, when provided
    * @param defaultReadPermission
    *   the default permission required in order to download a file from a s3 storage
    * @param defaultWritePermission
    *   the default permission required in order to upload a file to a s3 storage
    * @param showLocation
    *   flag to decide whether or not to show the absolute location of the files in the metadata response
    * @param defaultMaxFileSize
    *   the default maximum allowed file size (in bytes) for uploaded files
    * @param defaultBucket
    *   the bucket used for S3 storages if not specified during storage creation
    * @param prefix
    *   global prefix prepended to the path of all saved S3 files
    */
  final case class S3StorageConfig(
      defaultEndpoint: Uri,
      useDefaultCredentialProvider: Boolean,
      defaultAccessKey: Secret[String],
      defaultSecretKey: Secret[String],
      defaultReadPermission: Permission,
      defaultWritePermission: Permission,
      showLocation: Boolean,
      defaultMaxFileSize: Long,
      defaultBucket: String,
      prefix: Option[Path]
  ) extends StorageTypeEntryConfig {
    val prefixPath: Path = prefix.getOrElse(Path.empty)
  }

  implicit private val permissionConverter: ConfigConvert[Permission] =
    ConfigConvert.viaString[Permission](optF(Permission(_).toOption), _.toString)

  implicit private val digestAlgConverter: ConfigConvert[DigestAlgorithm] =
    ConfigConvert.viaString[DigestAlgorithm](optF(DigestAlgorithm(_)), _.toString)

  implicit private val absolutePathConverter: ConfigConvert[AbsolutePath] =
    ConfigConvert.viaString[AbsolutePath](
      str => AbsolutePath(str).leftMap(err => CannotConvert(str, "AbsolutePath", err)),
      _.toString
    )
}
