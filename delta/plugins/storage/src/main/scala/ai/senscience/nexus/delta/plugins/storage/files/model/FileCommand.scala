package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageType
import ai.senscience.nexus.delta.plugins.storage.storages.operations.StorageWrite
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}

/**
  * Enumeration of File command types.
  */
sealed trait FileCommand extends Product with Serializable {

  /**
    * @return
    *   the project where the file belongs to
    */
  def project: ProjectRef

  /**
    * @return
    *   the file identifier
    */
  def id: Iri

  /**
    * the last known revision of the file
    * @return
    */
  def rev: Int

  /**
    * @return
    *   the identity associated to this command
    */
  def subject: Subject
}

object FileCommand {

  /**
    * Command to create a new file
    *
    * @param id
    *   the file identifier
    * @param project
    *   the project the file belongs to
    * @param storage
    *   the reference to the used storage
    * @param storageType
    *   the type of storage
    * @param attributes
    *   the file attributes
    * @param subject
    *   the identity associated to this command
    * @param tag
    *   an optional user-specified tag attached to the file on creation
    */
  final case class CreateFile(
      id: Iri,
      project: ProjectRef,
      storage: ResourceRef.Revision,
      storageType: StorageType,
      attributes: FileAttributes,
      subject: Subject,
      tag: Option[UserTag]
  ) extends FileCommand {
    override def rev: Int = 0
  }

  object CreateFile {
    def apply(
        id: Iri,
        project: ProjectRef,
        storageWrite: StorageWrite[FileAttributes],
        subject: Subject,
        tag: Option[UserTag]
    ): CreateFile =
      CreateFile(id, project, storageWrite.storage, storageWrite.tpe, storageWrite.value, subject, tag)
  }

  /**
    * Command to update an existing file
    *
    * @param id
    *   the file identifier
    * @param project
    *   the project the file belongs to
    * @param storage
    *   the reference to the used storage
    * @param storageType
    *   the type of storage
    * @param attributes
    *   the file attributes
    * @param subject
    *   the identity associated to this command
    * @param tag
    *   an optional user-specified tag attached to the latest revision
    */
  final case class UpdateFile(
      id: Iri,
      project: ProjectRef,
      storage: ResourceRef.Revision,
      storageType: StorageType,
      attributes: FileAttributes,
      rev: Int,
      subject: Subject,
      tag: Option[UserTag]
  ) extends FileCommand

  object UpdateFile {
    def apply(
        id: Iri,
        project: ProjectRef,
        storageWrite: StorageWrite[FileAttributes],
        rev: Int,
        subject: Subject,
        tag: Option[UserTag]
    ): UpdateFile = UpdateFile(
      id,
      project,
      storageWrite.storage,
      storageWrite.tpe,
      storageWrite.value,
      rev,
      subject,
      tag
    )
  }

  /**
    * Command to update the custom metadata of a file
    *
    * @param id
    *   the file identifier
    * @param project
    *   the project the file belongs to
    * @param metadata
    *   the custom metadata to update
    * @param rev
    *   the last known revision of the file
    * @param subject
    *   the identity associated to this command
    * @param tag
    *   an optional user-specified tag attached to the latest revision
    */
  final case class UpdateFileCustomMetadata(
      id: Iri,
      project: ProjectRef,
      metadata: FileCustomMetadata,
      rev: Int,
      subject: Subject,
      tag: Option[UserTag]
  ) extends FileCommand

  /**
    * Command to tag a file
    *
    * @param id
    *   the file identifier
    * @param project
    *   the project the file belongs to
    * @param targetRev
    *   the revision that is being aliased with the provided ''tag''
    * @param tag
    *   the tag of the alias for the provided ''tagRev''
    * @param rev
    *   the last known revision of the file
    * @param subject
    *   the identity associated to this command
    */
  final case class TagFile(id: Iri, project: ProjectRef, targetRev: Int, tag: UserTag, rev: Int, subject: Subject)
      extends FileCommand

  /**
    * Command to delete a tag from a file
    *
    * @param id
    *   the file identifier
    * @param project
    *   the project the file belongs to
    * @param tag
    *   the tag to delete
    * @param rev
    *   the last known revision of the file
    * @param subject
    *   the identity associated to this command
    */
  final case class DeleteFileTag(id: Iri, project: ProjectRef, tag: UserTag, rev: Int, subject: Subject)
      extends FileCommand

  /**
    * Command to deprecate a file
    *
    * @param id
    *   the file identifier
    * @param project
    *   the project the file belongs to
    * @param rev
    *   the last known revision of the file
    * @param subject
    *   the identity associated to this command
    */
  final case class DeprecateFile(id: Iri, project: ProjectRef, rev: Int, subject: Subject) extends FileCommand

  /**
    * Command to undeprecate a file
    *
    * @param id
    *   the file identifier
    * @param project
    *   the project the file belongs to
    * @param rev
    *   the last known revision of the file
    * @param subject
    *   the identity associated to this command
    */
  final case class UndeprecateFile(id: Iri, project: ProjectRef, rev: Int, subject: Subject) extends FileCommand

  /**
    * Command to express an event which was dropped during a rewrite of the file history
    * @param id
    *   the file identifier
    * @param project
    *   the project the file belongs to
    * @param reason
    *   the reason for cancelling the event
    * @param rev
    *   the last known revision of the file
    * @param subject
    *   the identity associated to this command
    */
  final case class CancelEvent(id: Iri, project: ProjectRef, reason: String, rev: Int, subject: Subject)
      extends FileCommand
}
