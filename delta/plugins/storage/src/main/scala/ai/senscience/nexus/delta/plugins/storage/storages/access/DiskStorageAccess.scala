package ai.senscience.nexus.delta.plugins.storage.storages.access

import ai.senscience.nexus.delta.plugins.storage.storages.model.AbsolutePath
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageRejection.StorageNotAccessible
import cats.effect.IO

import java.nio.file.Files

object DiskStorageAccess {

  def checkVolumeExists(path: AbsolutePath): IO[Unit] = {
    def failWhen(condition: Boolean, err: => String) = {
      IO.raiseWhen(condition)(StorageNotAccessible(err))
    }

    for {
      exists      <- IO.blocking(Files.exists(path.value))
      _           <- failWhen(!exists, s"Volume '${path.value}' does not exist.")
      isDirectory <- IO.blocking(Files.isDirectory(path.value))
      _           <- failWhen(!isDirectory, s"Volume '${path.value}' is not a directory.")
      isWritable  <- IO.blocking(Files.isWritable(path.value))
      _           <- failWhen(!isWritable, s"Volume '${path.value}' does not have write access.")
    } yield ()
  }

}
