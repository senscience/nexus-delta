package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.projects.model.ProjectBase
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.Json

/**
  * Enumeration of composite view commands.
  */
sealed trait CompositeViewCommand extends Product with Serializable {

  /**
    * @return
    *   the view id
    */
  def id: Iri

  /**
    * @return
    *   a reference to the parent project
    */
  def project: ProjectRef

  /**
    * @return
    *   the last known revision of the view
    */
  def rev: Int

  /**
    * @return
    *   the identity associated with this command
    */
  def subject: Subject
}

object CompositeViewCommand {

  /**
    * Command for the creation of a new composite view.
    *
    * @param id
    *   the view id
    * @param project
    *   a reference to the parent project
    * @param value
    *   the view configuration
    * @param source
    *   the original json value provided by the caller
    * @param subject
    *   the identity associated with this command
    * @param projectBase
    *   project base used to generate ids
    */
  final case class CreateCompositeView(
      id: Iri,
      project: ProjectRef,
      value: CompositeViewFields,
      source: Json,
      subject: Subject,
      projectBase: ProjectBase
  ) extends CompositeViewCommand {
    override def rev: Int = 0
  }

  /**
    * Command for the update of a composite view.
    *
    * @param id
    *   the view id
    * @param project
    *   a reference to the parent project
    * @param rev
    *   the last known revision of the view
    * @param value
    *   the view configuration
    * @param source
    *   the original json value provided by the caller
    * @param subject
    *   the identity associated with this command
    * @param projectBase
    *   project base used to generate ids
    */
  final case class UpdateCompositeView(
      id: Iri,
      project: ProjectRef,
      rev: Int,
      value: CompositeViewFields,
      source: Json,
      subject: Subject,
      projectBase: ProjectBase
  ) extends CompositeViewCommand

  /**
    * Command for the deprecation of a composite view.
    *
    * @param id
    *   the view id
    * @param project
    *   a reference to the parent project
    * @param rev
    *   the last known revision of the view
    * @param subject
    *   the identity associated with this command
    */
  final case class DeprecateCompositeView(id: Iri, project: ProjectRef, rev: Int, subject: Subject)
      extends CompositeViewCommand

  /**
    * Command for the undeprecation of a composite view.
    *
    * @param id
    *   the view id
    * @param project
    *   a reference to the parent project
    * @param rev
    *   the last known revision of the view
    * @param subject
    *   the identity associated with this command
    */
  final case class UndeprecateCompositeView(id: Iri, project: ProjectRef, rev: Int, subject: Subject)
      extends CompositeViewCommand
}
