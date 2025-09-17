package ai.senscience.nexus.delta.plugins.storage.storages.operations.disk

import ai.senscience.nexus.delta.plugins.storage.files.model.Digest.ComputedDigest
import ai.senscience.nexus.delta.plugins.storage.files.model.FileAttributes.FileAttributesOrigin.Client
import ai.senscience.nexus.delta.plugins.storage.files.model.FileStorageMetadata
import ai.senscience.nexus.delta.plugins.storage.storages.UUIDFFixtures
import ai.senscience.nexus.delta.plugins.storage.storages.model.{AbsolutePath, DigestAlgorithm}
import ai.senscience.nexus.delta.plugins.storage.storages.operations.FileDataHelpers
import ai.senscience.nexus.delta.plugins.storage.storages.operations.StorageFileRejection.SaveFileRejection.ResourceAlreadyExists
import ai.senscience.nexus.delta.plugins.storage.storages.operations.UploadingFile.DiskUploadingFile
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.file.TempDirectory
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import fs2.io.file.{Files, Path}
import munit.AnyFixture
import org.http4s.Uri

class DiskStorageSaveFileSuite
    extends NexusSuite
    with FileDataHelpers
    with UUIDFFixtures.Fixed
    with TempDirectory.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(tempDirectory)

  private val project               = ProjectRef.unsafe("org", "project")
  private lazy val tempDir          = tempDirectory()
  private lazy val volume           = AbsolutePath.unsafe(tempDir.toNioPath)
  private val filePath              = Path(s"$project/8/0/4/9/b/a/9/0/myfile.txt")
  private lazy val absoluteFilePath = tempDir.resolve(filePath)

  private val fileOps = DiskFileOperations.mk

  private val content = "file content"
  private val digest  = "e0ac3601005dfa1864f5392aabaf7d898b1b5bab854f1acb4491bcd806b76b0c"
  private val data    = streamData(content)

  private lazy val uploading = DiskUploadingFile(project, volume, DigestAlgorithm.default, "myfile.txt", data)

  test("Save successfully the file to the volume") {
    for {
      result      <- fileOps.save(uploading)
      fileContent <- Files[IO].readUtf8(absoluteFilePath).assert(content)
      fileSize    <- Files[IO].size(absoluteFilePath)
    } yield {
      val expected = FileStorageMetadata(
        fixedUuid,
        fileSize,
        ComputedDigest(DigestAlgorithm.default, digest),
        Client,
        Uri.unsafeFromString(s"file://${absoluteFilePath.toString}"),
        Uri.Path.unsafeFromString(filePath.toString)
      )
      assertEquals(result, expected)
    }
  }

  test("Fail attempting to save the same file again") {
    fileOps.save(uploading).intercept[ResourceAlreadyExists]
  }
}
