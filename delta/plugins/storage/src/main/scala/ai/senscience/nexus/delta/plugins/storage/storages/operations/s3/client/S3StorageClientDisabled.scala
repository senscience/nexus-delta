package ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.client

import ai.senscience.nexus.delta.plugins.storage.files.model.MediaType
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.{CopyOptions, HeadObject, S3OperationResult}
import ai.senscience.nexus.delta.sdk.FileData
import ai.senscience.nexus.delta.sdk.error.ServiceError.FeatureDisabled
import cats.effect.IO
import fs2.Stream
import software.amazon.awssdk.services.s3.model.*

private[client] object S3StorageClientDisabled extends S3StorageClient {
  private val disabledErr      = FeatureDisabled("S3 storage is disabled")
  private val raiseDisabledErr = IO.raiseError(disabledErr)

  override def listObjectsV2(bucket: String): IO[ListObjectsV2Response] = raiseDisabledErr

  override def listObjectsV2(bucket: String, prefix: String): IO[ListObjectsV2Response] = raiseDisabledErr

  override def readFile(bucket: String, fileKey: String): FileData = Stream.raiseError[IO](disabledErr)

  override def headObject(bucket: String, key: String): IO[HeadObject] = raiseDisabledErr

  override def copyObject(
      sourceBucket: String,
      sourceKey: String,
      destinationBucket: String,
      destinationKey: String,
      options: CopyOptions
  ): IO[S3OperationResult] = raiseDisabledErr

  override def objectExists(bucket: String, key: String): IO[Boolean] = raiseDisabledErr

  override def uploadFile(
      putObjectRequest: s3.PutObjectRequest,
      data: FileData
  ): IO[Unit] = raiseDisabledErr

  override def updateContentType(bucket: String, key: String, mediaType: MediaType): IO[S3OperationResult] =
    raiseDisabledErr

  override def bucketExists(bucket: String): IO[Boolean] = raiseDisabledErr

  override def copyObjectMultiPart(
      sourceBucket: String,
      sourceKey: String,
      destinationBucket: String,
      destinationKey: String,
      options: CopyOptions
  ): IO[S3OperationResult] = raiseDisabledErr

  override def readFileMultipart(bucket: String, fileKey: String): Stream[IO, Byte] = throw disabledErr
}
