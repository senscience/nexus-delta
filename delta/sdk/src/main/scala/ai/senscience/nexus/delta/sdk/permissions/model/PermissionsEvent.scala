package ai.senscience.nexus.delta.sdk.permissions.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.event.Event.GlobalEvent
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Label
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant

/**
  * Enumeration of Permissions event types.
  */
sealed trait PermissionsEvent extends GlobalEvent {

  /**
    * The relative [[Iri]] of the permission
    */
  override def id: Iri = Permissions.id

}

object PermissionsEvent {

  /**
    * A witness to a collection of permissions appended to the set.
    *
    * @param rev
    *   the revision this event generated
    * @param permissions
    *   the collection of permissions appended to the set
    * @param instant
    *   the instant when the event was emitted
    * @param subject
    *   the subject that performed the action that resulted in emitting this event
    */
  final case class PermissionsAppended(
      rev: Int,
      permissions: Set[Permission],
      instant: Instant,
      subject: Subject
  ) extends PermissionsEvent

  /**
    * A witness to a collection of permissions subtracted from the set.
    *
    * @param rev
    *   the revision this event generated
    * @param permissions
    *   the collection of permissions subtracted from the set
    * @param instant
    *   the instant when the event was emitted
    * @param subject
    *   the subject that performed the action that resulted in emitting this event
    */
  final case class PermissionsSubtracted(
      rev: Int,
      permissions: Set[Permission],
      instant: Instant,
      subject: Subject
  ) extends PermissionsEvent

  /**
    * A witness to the permission set being replaced.
    *
    * @param rev
    *   the revision this event generated
    * @param permissions
    *   the new set of permissions that replaced the previous set
    * @param instant
    *   the instant when the event was emitted
    * @param subject
    *   the subject that performed the action that resulted in emitting this event
    */
  final case class PermissionsReplaced(
      rev: Int,
      permissions: Set[Permission],
      instant: Instant,
      subject: Subject
  ) extends PermissionsEvent

  /**
    * A witness to the permission set being deleted (emptied).
    *
    * @param rev
    *   the revision this event generated
    * @param instant
    *   the instant when the event was emitted
    * @param subject
    *   the subject that performed the action that resulted in emitting this event
    */
  final case class PermissionsDeleted(
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends PermissionsEvent

  val serializer: Serializer[Label, PermissionsEvent] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration            = Serializer.circeConfiguration
    implicit val coder: Codec.AsObject[PermissionsEvent] = deriveConfiguredCodec[PermissionsEvent]
    Serializer(_ => Permissions.id)
  }
}
