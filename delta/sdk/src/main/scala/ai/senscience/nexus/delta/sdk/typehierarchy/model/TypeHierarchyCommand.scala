package ai.senscience.nexus.delta.sdk.typehierarchy.model

import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchy.TypeHierarchyMapping
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject

/**
  * Enumeration of TypeHierarchy collection command types.
  */
sealed trait TypeHierarchyCommand {
  def mapping: TypeHierarchyMapping
  def subject: Subject
}

object TypeHierarchyCommand {

  /** An intent to create a type hierarchy. */
  final case class CreateTypeHierarchy(mapping: TypeHierarchyMapping, subject: Subject) extends TypeHierarchyCommand

  /** An intent to update a type hierarchy. */
  final case class UpdateTypeHierarchy(mapping: TypeHierarchyMapping, rev: Int, subject: Subject)
      extends TypeHierarchyCommand
}
