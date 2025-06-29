package ai.senscience.nexus.delta.plugins.storage.storages.operations

import ai.senscience.nexus.delta.plugins.storage.files.model.*
import ai.senscience.nexus.delta.plugins.storage.files.model.FileAttributes.FileAttributesOrigin
import ai.senscience.nexus.delta.plugins.storage.files.model.FileRejection.InvalidFilePath
import ai.senscience.nexus.delta.plugins.storage.files.{permissions, MediaTypeDetector, MediaTypeDetectorConfig}
import ai.senscience.nexus.delta.plugins.storage.storages.FetchStorage
import ai.senscience.nexus.delta.plugins.storage.storages.model.Storage.S3Storage
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageValue.S3StorageValue
import ai.senscience.nexus.delta.plugins.storage.storages.model.{Storage, StorageType}
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.S3FileOperations.{S3FileLink, S3FileMetadata}
import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import io.circe.Json
import org.http4s.Uri

import java.util.UUID

class LinkFileActionSuite extends NexusSuite {

  private val realm           = Label.unsafe("myrealm")
  private val user            = User("user", realm)
  implicit val caller: Caller = Caller(user, Set.empty)

  private val project    = ProjectRef.unsafe("org", "project")
  private val storageIri = nxv + "s3-storage"
  private val storageRef = ResourceRef.Revision(storageIri, 1)
  private val value      = S3StorageValue(default = false, "bucket", permissions.read, permissions.write, 100L)
  private val s3Storage  = S3Storage(storageIri, project, value, Json.Null)

  private val fetchStorage = new FetchStorage {

    override def onRead(id: ResourceRef, project: ProjectRef)(implicit caller: Caller): IO[Storage] =
      IO.raiseError(new IllegalStateException("Should not be called"))

    override def onWrite(id: Option[IriOrBNode.Iri], project: ProjectRef)(implicit
        caller: Caller
    ): IO[(ResourceRef.Revision, Storage)] =
      IO.raiseUnless(id.contains(storageIri))(AuthorizationFailed("Fail")) >> IO.pure(storageRef -> s3Storage)
  }

  private val mediaTypeDetector = new MediaTypeDetector(MediaTypeDetectorConfig.Empty)

  private val uuid            = UUID.randomUUID()
  private val mediaTypeFromS3 = MediaType.`application/octet-stream`
  private val fileSize        = 100L
  private val digest          = Digest.NoDigest

  private val s3FileLink: S3FileLink = (_: String, path: Uri.Path) => {
    IO.fromOption(path.lastSegment)(InvalidFilePath).map { filename =>
      S3FileMetadata(
        filename,
        Some(mediaTypeFromS3),
        FileStorageMetadata(
          uuid,
          fileSize,
          digest,
          FileAttributesOrigin.Link,
          Uri(path = path),
          path
        )
      )
    }
  }

  private val linkAction = LinkFileAction(fetchStorage, mediaTypeDetector, s3FileLink)

  test("Fail for an unauthorized storage") {
    val request = FileLinkRequest(Uri.Path.unsafeFromString("/path/file.json"), None, None)
    linkAction(None, project, request).intercept[AuthorizationFailed]
  }

  test("Succeed for a file with media type detection") {
    val request    = FileLinkRequest(Uri.Path.unsafeFromString("/path/file.json"), None, None)
    val attributes = FileAttributes(
      uuid,
      Uri(path = request.path),
      request.path,
      "file.json",
      Some(MediaType.`application/json`),
      Map.empty,
      None,
      None,
      fileSize,
      digest,
      FileAttributesOrigin.Link
    )
    val expected   = StorageWrite(storageRef, StorageType.S3Storage, attributes)
    linkAction(Some(storageIri), project, request).assertEquals(expected)
  }

  test("Succeed for a file without media type detection") {
    val request    = FileLinkRequest(Uri.Path.unsafeFromString("/path/file.unknown"), None, None)
    val attributes = FileAttributes(
      uuid,
      Uri(path = request.path),
      request.path,
      "file.unknown",
      Some(mediaTypeFromS3),
      Map.empty,
      None,
      None,
      fileSize,
      digest,
      FileAttributesOrigin.Link
    )
    val expected   = StorageWrite(storageRef, StorageType.S3Storage, attributes)
    linkAction(Some(storageIri), project, request).assertEquals(expected)
  }

  test("Succeed for a file with provided media type") {
    val customMediaType = MediaType("application", "obj")
    val request         = FileLinkRequest(Uri.Path.unsafeFromString("/path/file.obj"), Some(customMediaType), None)
    val attributes      = FileAttributes(
      uuid,
      Uri(path = request.path),
      request.path,
      "file.obj",
      Some(customMediaType),
      Map.empty,
      None,
      None,
      fileSize,
      digest,
      FileAttributesOrigin.Link
    )
    val expected        = StorageWrite(storageRef, StorageType.S3Storage, attributes)
    linkAction(Some(storageIri), project, request).assertEquals(expected)
  }

}
