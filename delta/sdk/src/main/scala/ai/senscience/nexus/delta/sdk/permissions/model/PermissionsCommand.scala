package ai.senscience.nexus.delta.sdk.permissions.model

import ai.senscience.nexus.delta.sourcing.model.Identity.Subject

/**
  * Enumeration of Permissions command types.
  */
sealed trait PermissionsCommand extends Product with Serializable {

  /**
    * @return
    *   the current revision of the permission resource
    */
  def rev: Int

  /**
    * @return
    *   the subject that intends to evaluate this command
    */
  def subject: Subject
}

object PermissionsCommand {

  /**
    * An intent to replace the current permissions collection with a new ''permissions'' collection.
    *
    * @param rev
    *   the expected current revision of the resource
    * @param permissions
    *   the new collection of permissions
    * @param subject
    *   the subject that intends to evaluate this command
    */
  final case class ReplacePermissions(
      rev: Int,
      permissions: Set[Permission],
      subject: Subject
  ) extends PermissionsCommand

  /**
    * An intent to append the provided ''permissions'' collection to the permissions set.
    *
    * @param rev
    *   the expected current revision of the resource
    * @param permissions
    *   the collection of permissions to be appended
    * @param subject
    *   the subject that intends to evaluate this command
    */
  final case class AppendPermissions(
      rev: Int,
      permissions: Set[Permission],
      subject: Subject
  ) extends PermissionsCommand

  /**
    * An intent to subtract the provided ''permissions'' collection from the permissions set.
    *
    * @param rev
    *   the expected current revision of the resource
    * @param permissions
    *   the collection of permissions to be subtracted
    * @param subject
    *   the subject that intends to evaluate this command
    */
  final case class SubtractPermissions(
      rev: Int,
      permissions: Set[Permission],
      subject: Subject
  ) extends PermissionsCommand

  /**
    * An intent to delete (empty) the current permissions set.
    *
    * @param rev
    *   the expected current revision of the resource
    * @param subject
    *   the subject that intends to evaluate this command
    */
  final case class DeletePermissions(
      rev: Int,
      subject: Subject
  ) extends PermissionsCommand
}
