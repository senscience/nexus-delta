package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugins.storage.storages.StorageDeletionTask.{init, logger}
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageValue
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageValue.{DiskStorageValue, S3StorageValue}
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import fs2.Stream
import fs2.io.file.Files

/**
  * Creates a project deletion step that deletes the directory associated to a local storage and all the physical files
  * written under it. No deletion occurs for other types of storage.
  */
final class StorageDeletionTask(currentStorages: ProjectRef => Stream[IO, StorageValue]) extends ProjectDeletionTask {

  override def apply(project: ProjectRef)(implicit subject: Subject): IO[ProjectDeletionReport.Stage] =
    logger.info(s"Starting deletion of local files for $project") >>
      run(project)

  private def run(project: ProjectRef) =
    currentStorages(project)
      .evalScan(init) {
        case (acc, disk: DiskStorageValue) =>
          deleteRecursively(project, disk).map(acc ++ _)
        case (acc, s3: S3StorageValue)     =>
          val message =
            s"Deletion of files for S3 storages is yet to be implemented. Files in bucket '${s3.bucket}' will remain."
          logger.warn(message).as(acc ++ message)
      }
      .compile
      .lastOrError

  private def deleteRecursively(project: ProjectRef, disk: DiskStorageValue) = {
    val directory = disk.rootDirectory(project)
    Files[IO].exists(directory).flatMap {
      case true  =>
        Files[IO].deleteRecursively(directory).as(s"Local directory '$directory' has been deleted.")
      case false =>
        IO.pure(s"Local directory '$directory' does no exist.")
    }
  }

}

object StorageDeletionTask {

  private val logger = Logger[StorageDeletionTask]

  private val init = ProjectDeletionReport.Stage.empty("storage")

  def apply(storages: Storages) =
    new StorageDeletionTask(project =>
      storages
        .currentStorages(project)
        .map(_.value.value)
    )
}
