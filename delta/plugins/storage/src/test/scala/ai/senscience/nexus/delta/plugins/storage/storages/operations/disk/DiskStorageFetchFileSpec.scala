package ai.senscience.nexus.delta.plugins.storage.storages.operations.disk

import ai.senscience.nexus.delta.plugins.storage.storages.UUIDFFixtures
import ai.senscience.nexus.delta.plugins.storage.storages.operations.FileDataHelpers
import ai.senscience.nexus.delta.plugins.storage.storages.operations.StorageFileRejection.FetchFileRejection
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import org.http4s.Uri

import java.nio.file.Files

class DiskStorageFetchFileSpec extends CatsEffectSpec with FileDataHelpers with UUIDFFixtures.Random {

  private val fileOps = DiskFileOperations.mk

  "A DiskStorage fetching operations" should {

    "fetch a file from a volume" in {
      val volume = Files.createTempDirectory("disk-access")
      val file   = volume.resolve("my/file.txt")
      Files.createDirectories(file.getParent)
      Files.createFile(file)
      Files.writeString(file, "file content")

      val data = fileOps.fetch(Uri.Path.unsafeFromString(file.toString))
      consume(data).accepted shouldEqual "file content"
      Files.delete(file)
    }

    "deal with a missing file" in {
      val volume       = Files.createTempDirectory("disk-access")
      val file         = volume.resolve("my/file.txt")
      Files.createDirectories(file.getParent)
      val fullFilePath = file.toString

      fileOps.fetch(Uri.Path.unsafeFromString(fullFilePath)).compile.lastOrError.rejected shouldEqual FetchFileRejection
        .FileNotFound(fullFilePath)
    }
  }
}
