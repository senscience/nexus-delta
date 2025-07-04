package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sdk.ProjectResource
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, Decoder, Encoder}

import java.time.Instant
import java.util.UUID

/**
  * State used for all resources that have been created and later possibly updated or deprecated.
  *
  * @param label
  *   the project label
  * @param uuid
  *   the project unique identifier
  * @param organizationLabel
  *   the parent organization label
  * @param organizationUuid
  *   the parent organization uuid
  * @param rev
  *   the current state revision
  * @param deprecated
  *   the current state deprecation status
  * @param markedForDeletion
  *   the current marked for deletion status
  * @param description
  *   an optional project description
  * @param apiMappings
  *   the project API mappings
  * @param base
  *   the base Iri for generated resource IDs
  * @param vocab
  *   an optional vocabulary for resources with no context
  * @param enforceSchema
  *   a flag to ban unconstrained resources in this project
  * @param createdAt
  *   the instant when the resource was created
  * @param createdBy
  *   the subject that created the resource
  * @param updatedAt
  *   the instant when the resource was last updated
  * @param updatedBy
  *   the subject that last updated the resource
  */
final case class ProjectState(
    label: Label,
    uuid: UUID,
    organizationLabel: Label,
    organizationUuid: UUID,
    rev: Int,
    deprecated: Boolean,
    markedForDeletion: Boolean,
    description: Option[String],
    apiMappings: ApiMappings,
    base: ProjectBase,
    vocab: Iri,
    enforceSchema: Boolean = false,
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends ScopedState {

  override val project: ProjectRef = ProjectRef(organizationLabel, label)

  /**
    * The relative [[Iri]] of the project
    */
  override def id: Iri = Projects.encodeId(project)

  /**
    * @return
    *   the schema reference that projects conforms to
    */
  def schema: ResourceRef = Latest(schemas.projects)

  /**
    * @return
    *   the collection of known types of projects resources
    */
  def types: Set[Iri] = Set(nxv.Project)

  /**
    * Converts the state into a resource representation.
    */
  def toResource(defaultApiMappings: ApiMappings): ProjectResource =
    ResourceF(
      id = id,
      access = ResourceAccess.project(project),
      rev = rev,
      types = types,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      schema = schema,
      value = Project(
        label,
        uuid,
        organizationLabel,
        organizationUuid,
        description,
        apiMappings,
        defaultApiMappings,
        base,
        vocab,
        enforceSchema,
        markedForDeletion
      )
    )
}

object ProjectState {

  implicit val serializer: Serializer[ProjectRef, ProjectState] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    // TODO: The `.withDefaults` method is used in order to inject the default empty remoteContexts
    //  when deserializing an event that has none. Remove it after 1.10 migration.
    implicit val configuration: Configuration = Serializer.circeConfiguration.withDefaults

    implicit val apiMappingsDecoder: Decoder[ApiMappings]          =
      Decoder.decodeMap[String, Iri].map(ApiMappings(_))
    implicit val apiMappingsEncoder: Encoder.AsObject[ApiMappings] =
      Encoder.encodeMap[String, Iri].contramapObject(_.value)

    implicit val coder: Codec.AsObject[ProjectState] = deriveConfiguredCodec[ProjectState]
    Serializer.dropNullsInjectType(Projects.encodeId)
  }

}
