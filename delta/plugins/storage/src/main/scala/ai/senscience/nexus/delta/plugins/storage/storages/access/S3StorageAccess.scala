package ai.senscience.nexus.delta.plugins.storage.storages.access

import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageRejection.StorageNotAccessible
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.client.S3StorageClient
import cats.effect.IO

trait S3StorageAccess {

  def checkBucketExists(bucket: String): IO[Unit]

}

object S3StorageAccess {

  def apply(client: S3StorageClient): S3StorageAccess =
    (bucket: String) =>
      client.bucketExists(bucket).flatMap { exists =>
        IO.raiseUnless(exists)(StorageNotAccessible(s"Bucket $bucket does not exist"))
      }

}
