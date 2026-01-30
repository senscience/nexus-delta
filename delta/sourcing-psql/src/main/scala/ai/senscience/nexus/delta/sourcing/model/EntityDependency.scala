package ai.senscience.nexus.delta.sourcing.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import cats.Order

/**
  * Defines a dependency to another entity
  */
object EntityDependency {

  /**
    * Defines an entity that is referenced by a project or entity
    */
  final case class ReferencedBy(project: ProjectRef, id: Iri)

  /**
    * Defines an entity that a project / entity depends on
    */
  final case class DependsOn(project: ProjectRef, id: Iri)

  object DependsOn {
    given Order[DependsOn] = Order.by { dependency =>
      (dependency.project, dependency.id)
    }
  }
}
