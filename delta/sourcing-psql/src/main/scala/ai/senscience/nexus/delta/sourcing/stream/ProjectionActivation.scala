package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef

/**
  * An instruction, broadcast to the view coordinators, to (re)start projections. Coordinators react by recompiling and
  * running the matching projections through the supervisor.
  */
sealed trait ProjectionActivation extends Product with Serializable

object ProjectionActivation {

  /**
    * Start every projection the coordinators hold for the given project (e.g. when a project becomes active again).
    */
  final case class ForProject(project: ProjectRef) extends ProjectionActivation

  /**
    * Start the single projection identified by the given metadata. The `module` allows each coordinator to cheaply
    * ignore activations that are not its own.
    */
  final case class ForProjection(metadata: ProjectionMetadata) extends ProjectionActivation

}
