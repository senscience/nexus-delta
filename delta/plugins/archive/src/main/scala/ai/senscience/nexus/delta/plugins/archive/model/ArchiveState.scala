package ai.senscience.nexus.delta.plugins.archive.model

import ai.senscience.nexus.delta.plugins.archive.{model, schema as archiveSchema, tpe, ArchiveResource}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.instances.*
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF, ResourceRepresentation}
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.state.State.EphemeralState
import cats.data.NonEmptySet
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/**
  * State of an existing archive.
  *
  * @param id
  *   the archive identifier
  * @param project
  *   the archive parent project
  * @param resources
  *   the collection of referenced resources
  * @param createdAt
  *   the instant when the archive was created
  * @param createdBy
  *   the subject that created the archive
  */
final case class ArchiveState(
    id: Iri,
    project: ProjectRef,
    resources: NonEmptySet[ArchiveReference],
    createdAt: Instant,
    createdBy: Subject
) extends EphemeralState {

  override def schema: ResourceRef = archiveSchema

  override def types: Set[Iri] = Set(tpe)

  def toResource(ttl: FiniteDuration): ArchiveResource =
    ResourceF(
      id = id,
      access = ResourceAccess.ephemeral("archives", project, id),
      rev = this.rev,
      types = this.types,
      deprecated = this.deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = createdAt,
      updatedBy = createdBy,
      schema = this.schema,
      value = Archive(id, project, resources, ttl.toSeconds)
    )
}

object ArchiveState {

  implicit val serializer: Serializer[Iri, ArchiveState] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration                                          = Serializer.circeConfiguration
    implicit val archiveResourceRepresentation: Codec.AsObject[ResourceRepresentation] =
      deriveConfiguredCodec[ResourceRepresentation]
    implicit val archiveReferenceCodec: Codec.AsObject[ArchiveReference]               = deriveConfiguredCodec[ArchiveReference]
    implicit val codec: Codec.AsObject[ArchiveState]                                   = deriveConfiguredCodec[ArchiveState]
    Serializer()
  }

}
