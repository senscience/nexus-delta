package ai.senscience.nexus.delta.sdk.organizations.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.organizations.Organizations
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.event.Event.GlobalEvent
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Label
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant
import java.util.UUID

/**
  * Enumeration of organization event states
  */
sealed trait OrganizationEvent extends GlobalEvent {

  /**
    * The relative [[Iri]] of the organization
    */
  def id: Iri = Organizations.encodeId(label)

  /**
    * @return
    *   the organization Label
    */
  def label: Label

  /**
    * @return
    *   the organization UUID
    */
  def uuid: UUID
}

object OrganizationEvent {

  /**
    * Event representing organization creation.
    *
    * @param label
    *   the organization label
    * @param uuid
    *   the organization UUID
    * @param rev
    *   the organization revision
    * @param description
    *   an optional description of the organization
    * @param instant
    *   the instant when this event was created
    * @param subject
    *   the subject which created this event
    */
  final case class OrganizationCreated(
      label: Label,
      uuid: UUID,
      rev: Int,
      description: Option[String],
      instant: Instant,
      subject: Subject
  ) extends OrganizationEvent

  /**
    * Event representing organization update.
    *
    * @param label
    *   the organization label
    * @param uuid
    *   the organization UUID
    * @param rev
    *   the update revision
    * @param description
    *   an optional description of the organization
    * @param instant
    *   the instant when this event was created
    * @param subject
    *   the subject which created this event
    */
  final case class OrganizationUpdated(
      label: Label,
      uuid: UUID,
      rev: Int,
      description: Option[String],
      instant: Instant,
      subject: Subject
  ) extends OrganizationEvent

  /**
    * Event representing organization deprecation.
    *
    * @param label
    *   the organization label
    * @param uuid
    *   the organization UUID
    * @param rev
    *   the deprecation revision
    * @param instant
    *   the instant when this event was created
    * @param subject
    *   the subject which created this event
    */
  final case class OrganizationDeprecated(
      label: Label,
      uuid: UUID,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends OrganizationEvent

  /**
    * Event representing organization undeprecation.
    *
    * @param label
    *   the organization label
    * @param uuid
    *   the organization UUID
    * @param rev
    *   the deprecation revision
    * @param instant
    *   the instant when this event was created
    * @param subject
    *   the subject which created this event
    */
  final case class OrganizationUndeprecated(
      label: Label,
      uuid: UUID,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends OrganizationEvent

  val serializer: Serializer[Label, OrganizationEvent] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration             = Serializer.circeConfiguration
    implicit val coder: Codec.AsObject[OrganizationEvent] = deriveConfiguredCodec[OrganizationEvent]
    Serializer(Organizations.encodeId)
  }
}
