package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.ProjectRef

/**
  * To filter projections when retrieving their offset or their errors
  */
sealed trait ProjectionSelector extends Product with Serializable

object ProjectionSelector {

  /**
    * Get projection by name
    */
  final case class Name(value: String) extends ProjectionSelector

  /**
    * Get projection by project and id
    */
  final case class ProjectId(project: ProjectRef, id: Iri) extends ProjectionSelector

}
