package ai.senscience.nexus.delta.plugins.storage.storages.operations.s3

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.utils.{UUIDF, UrlUtils}
import ai.senscience.nexus.delta.plugins.storage.files.model.FileAttributes.FileAttributesOrigin.Client
import ai.senscience.nexus.delta.plugins.storage.files.model.FileStorageMetadata
import ai.senscience.nexus.delta.plugins.storage.storages.operations.StorageFileRejection.SaveFileRejection
import ai.senscience.nexus.delta.plugins.storage.storages.operations.StorageFileRejection.SaveFileRejection.*
import ai.senscience.nexus.delta.plugins.storage.storages.operations.UploadingFile.S3UploadingFile
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.client.S3StorageClient
import cats.effect.IO
import org.http4s.{Status, Uri}
import software.amazon.awssdk.services.s3.model.S3Exception

import java.util.UUID

final class S3StorageSaveFile(s3StorageClient: S3StorageClient, locationGenerator: S3LocationGenerator)(implicit
    uuidf: UUIDF
) {

  private val logger = Logger[S3StorageSaveFile]

  def save(uploading: S3UploadingFile): IO[FileStorageMetadata] =
    uuidf().flatMap { uuid =>
      val location = locationGenerator.file(uploading.project, uuid, uploading.filename)
      storeFile(uploading, location, uuid)
    }

  private def storeFile(uploading: S3UploadingFile, location: Uri, uuid: UUID): IO[FileStorageMetadata] = {
    val put    =
      PutObjectRequest(
        uploading.bucket,
        UrlUtils.decodeUriPath(location.path),
        uploading.mediaType,
        uploading.contentLength
      )
    val bucket = put.bucket
    val key    = put.key
    (for {
      _            <- validateObjectDoesNotExist(bucket, key)
      _            <- s3StorageClient.uploadFile(put, uploading.data)
      headResponse <- s3StorageClient.headObject(bucket, key)
      attr          = fileMetadata(location, uuid, headResponse)
    } yield attr)
      .onError { case e => logger.error(e)("Unexpected error when storing file") }
      .adaptError {
        case e: SaveFileRejection                                      => e
        case e: S3Exception if e.statusCode() == Status.Forbidden.code =>
          BucketAccessDenied(bucket, key, e.getMessage)
        case e                                                         => UnexpectedSaveError(key, e.getMessage)
      }
  }

  private def fileMetadata(
      location: Uri,
      uuid: UUID,
      headResponse: HeadObject
  ): FileStorageMetadata =
    FileStorageMetadata(
      uuid = uuid,
      bytes = headResponse.fileSize,
      digest = headResponse.digest,
      origin = Client,
      location = location,
      path = location.path
    )

  private def validateObjectDoesNotExist(bucket: String, key: String) =
    s3StorageClient
      .objectExists(bucket, key)
      .flatMap {
        case true  => IO.raiseError(ResourceAlreadyExists(key))
        case false => IO.unit
      }
}
