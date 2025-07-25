package ai.senscience.nexus.delta.plugins.blazegraph.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.Json

/**
  * Enumeration of Blazegraph view commands.
  */
sealed trait BlazegraphViewCommand extends Product with Serializable {

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

object BlazegraphViewCommand {

  /**
    * Command for creating a new BlazegraphView.
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
    */
  final case class CreateBlazegraphView(
      id: Iri,
      project: ProjectRef,
      value: BlazegraphViewValue,
      source: Json,
      subject: Subject
  ) extends BlazegraphViewCommand {
    override def rev: Int = 0
  }

  /**
    * Command for the update of a BlazegraphView.
    *
    * @param id
    *   the view id
    * @param project
    *   a reference to the parent project
    * @param value
    *   the view configuration
    * @param rev
    *   the last known revision of the view
    * @param source
    *   the original json value provided by the caller
    * @param subject
    *   the identity associated with this command
    */
  final case class UpdateBlazegraphView(
      id: Iri,
      project: ProjectRef,
      value: BlazegraphViewValue,
      rev: Int,
      source: Json,
      subject: Subject
  ) extends BlazegraphViewCommand

  /**
    * Command for the deprecation of a BlazegraphView.
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
  final case class DeprecateBlazegraphView(id: Iri, project: ProjectRef, rev: Int, subject: Subject)
      extends BlazegraphViewCommand

  /**
    * Command for the undeprecation of a BlazegraphView.
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
  final case class UndeprecateBlazegraphView(id: Iri, project: ProjectRef, rev: Int, subject: Subject)
      extends BlazegraphViewCommand
}
