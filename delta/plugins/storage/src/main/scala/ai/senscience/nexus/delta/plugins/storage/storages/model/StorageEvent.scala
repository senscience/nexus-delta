package ai.senscience.nexus.delta.plugins.storage.storages.model

import ai.senscience.nexus.delta.plugins.storage.storages.{contexts, schemas, Storages}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.sdk.instances.*
import ai.senscience.nexus.delta.sdk.jsonld.IriEncoder
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.event.Event.ScopedEvent
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Label, ProjectRef}
import io.circe.*
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredCodec, deriveConfiguredEncoder}
import io.circe.syntax.*

import java.time.Instant

/**
  * Enumeration of Storage event types.
  */
sealed trait StorageEvent extends ScopedEvent {

  /**
    * @return
    *   the storage identifier
    */
  def id: Iri

  /**
    * @return
    *   the project where the storage belongs to
    */
  def project: ProjectRef

  /**
    * @return
    *   the storage type
    */
  def tpe: StorageType
}

object StorageEvent {

  /**
    * Event for the creation of a storage
    *
    * @param id
    *   the storage identifier
    * @param project
    *   the project the storage belongs to
    * @param value
    *   additional fields to configure the storage
    * @param instant
    *   the instant this event was created
    * @param subject
    *   the subject which created this event
    */
  final case class StorageCreated(
      id: Iri,
      project: ProjectRef,
      value: StorageValue,
      source: Json,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends StorageEvent {
    override val tpe: StorageType = value.tpe
  }

  /**
    * Event for the modification of an existing storage
    *
    * @param id
    *   the storage identifier
    * @param project
    *   the project the storage belongs to
    * @param value
    *   additional fields to configure the storage
    * @param rev
    *   the last known revision of the storage
    * @param instant
    *   the instant this event was created
    * @param subject
    *   the subject which created this event
    */
  final case class StorageUpdated(
      id: Iri,
      project: ProjectRef,
      value: StorageValue,
      source: Json,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends StorageEvent {
    override val tpe: StorageType = value.tpe
  }

  /**
    * Event for to tag a storage
    *
    * @param id
    *   the storage identifier
    * @param project
    *   the project the storage belongs to
    * @param tpe
    *   the storage type
    * @param targetRev
    *   the revision that is being aliased with the provided ''tag''
    * @param tag
    *   the tag of the alias for the provided ''tagRev''
    * @param rev
    *   the last known revision of the storage
    * @param instant
    *   the instant this event was created
    * @param subject
    *   the subject creating this event
    */
  final case class StorageTagAdded(
      id: Iri,
      project: ProjectRef,
      tpe: StorageType,
      targetRev: Int,
      tag: UserTag,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends StorageEvent

  /**
    * Event for the deprecation of a storage
    *
    * @param id
    *   the storage identifier
    * @param project
    *   the project the storage belongs to
    * @param tpe
    *   the storage type
    * @param rev
    *   the last known revision of the storage
    * @param instant
    *   the instant this event was created
    * @param subject
    *   the subject creating this event
    */
  final case class StorageDeprecated(
      id: Iri,
      project: ProjectRef,
      tpe: StorageType,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends StorageEvent

  /**
    * Event for the undeprecation of a storage
    *
    * @param id
    *   the storage identifier
    * @param project
    *   the project the storage belongs to
    * @param tpe
    *   the storage type
    * @param rev
    *   the last known revision of the storage
    * @param instant
    *   the instant this event was created
    * @param subject
    *   the subject creating this event
    */
  final case class StorageUndeprecated(
      id: Iri,
      project: ProjectRef,
      tpe: StorageType,
      rev: Int,
      instant: Instant,
      subject: Subject
  ) extends StorageEvent

  def serializer: Serializer[Iri, StorageEvent] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration = Serializer.circeConfiguration

    implicit val storageValueCodec: Codec.AsObject[StorageValue] = StorageValue.databaseCodec
    implicit val coder: Codec.AsObject[StorageEvent]             = deriveConfiguredCodec[StorageEvent]
    Serializer.dropNulls()
  }

  def sseEncoder(implicit base: BaseUri): SseEncoder[StorageEvent] = new SseEncoder[StorageEvent] {
    override val databaseDecoder: Decoder[StorageEvent] = serializer.codec

    override def entityType: EntityType = Storages.entityType

    override val selectors: Set[Label] = Set(Label.unsafe("storages"))

    override val sseEncoder: Encoder.AsObject[StorageEvent] = {
      val context = ContextValue(Vocabulary.contexts.metadata, contexts.storages)

      implicit val config: Configuration = Configuration.default
        .withDiscriminator(keywords.tpe)
        .copy(transformMemberNames = {
          case "id"      => "_storageId"
          case "source"  => nxv.source.prefix
          case "project" => nxv.project.prefix
          case "rev"     => nxv.rev.prefix
          case "instant" => nxv.instant.prefix
          case "subject" => nxv.eventSubject.prefix
          case other     => other
        })

      implicit val subjectEncoder: Encoder[Subject]           = IriEncoder.jsonEncoder[Subject]
      implicit val storageValueEncoder: Encoder[StorageValue] = Encoder.instance[StorageValue](_ => Json.Null)

      Encoder.encodeJsonObject.contramapObject { event =>
        deriveConfiguredEncoder[StorageEvent]
          .encodeObject(event)
          .remove("tpe")
          .remove("value")
          .add(nxv.types.prefix, event.tpe.types.asJson)
          .add(nxv.constrainedBy.prefix, schemas.storage.asJson)
          .add(nxv.resourceId.prefix, event.id.asJson)
          .add(keywords.context, context.value)
      }
    }
  }
}
