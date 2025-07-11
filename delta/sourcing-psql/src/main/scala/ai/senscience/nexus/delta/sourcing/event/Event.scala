package ai.senscience.nexus.delta.sourcing.event

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}

import java.time.Instant

/**
  * Super type of all events.
  */
sealed trait Event extends Product with Serializable {

  /**
    * The id of the event
    */
  def id: Iri

  /**
    * @return
    *   the revision this events generates
    */
  def rev: Int

  /**
    * @return
    *   the instant when the event was emitted
    */
  def instant: Instant

  /**
    * @return
    *   the subject that performed the action that resulted in emitting this event
    */
  def subject: Subject

}

object Event {

  /**
    * Event for entities that are transversal to all projects
    */
  trait GlobalEvent extends Event

  /**
    * Event for entities living inside a project
    */
  trait ScopedEvent extends Event {

    /**
      * @return
      *   the project where the event belongs
      */
    def project: ProjectRef

    /**
      * @return
      *   the parent organization label
      */
    def organization: Label = project.organization

  }
}
