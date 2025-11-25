package ai.senscience.nexus.delta.sdk.typehierarchy.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.typehierarchy.TypeHierarchy.typeHierarchyId
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchy.TypeHierarchyMapping
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.event.Event.GlobalEvent
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant

/**
  * Enumeration of TypeHierarchy collection event types.
  */
sealed trait TypeHierarchyEvent extends GlobalEvent {
  def id: Iri = typeHierarchyId
  def mapping: TypeHierarchyMapping
}

object TypeHierarchyEvent {

  /**
    * Event representing the creation of a type hierarchy.
    *
    * @param id
    *   the identifier of the type hierarchy
    * @param mapping
    *   the type hierarchy mapping
    * @param rev
    *   the revision number
    * @param instant
    *   the instant when the event was recorded
    * @param subject
    *   the subject that created the event
    */
  final case class TypeHierarchyCreated(
      mapping: TypeHierarchyMapping,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends TypeHierarchyEvent

  /**
    * Event representing the update of a type hierarchy.
    *
    * @param id
    *   the identifier of the type hierarchy
    * @param mapping
    *   the type hierarchy mapping
    * @param rev
    *   the revision number
    * @param instant
    *   the instant when the event was recorded
    * @param subject
    *   the subject that created the event
    */
  final case class TypeHierarchyUpdated(
      mapping: TypeHierarchyMapping,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends TypeHierarchyEvent

  val serializer: Serializer[Iri, TypeHierarchyEvent] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.given
    given Configuration                      = Serializer.circeConfiguration
    given Codec.AsObject[TypeHierarchyEvent] = deriveConfiguredCodec[TypeHierarchyEvent]
    Serializer(identity)
  }

}
