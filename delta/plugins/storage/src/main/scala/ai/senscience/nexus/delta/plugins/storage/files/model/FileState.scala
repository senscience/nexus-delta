package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.plugins.storage.files.{nxvFile, schemas, FileResource}
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageType
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef, Tags}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredCodec, deriveConfiguredDecoder}

import java.time.Instant

/**
  * State for an existing file
  *
  * @param id
  *   the id of the file
  * @param project
  *   the project it belongs to
  * @param storage
  *   the reference to the used storage
  * @param storageType
  *   the type of storage
  * @param attributes
  *   the file attributes
  * @param tags
  *   the collection of tag aliases
  * @param rev
  *   the current state revision
  * @param deprecated
  *   the current state deprecation status
  * @param createdAt
  *   the instant when the resource was created
  * @param createdBy
  *   the subject that created the resource
  * @param updatedAt
  *   the instant when the resource was last updated
  * @param updatedBy
  *   the subject that last updated the resource
  */
final case class FileState(
    id: Iri,
    project: ProjectRef,
    storage: ResourceRef.Revision,
    storageType: StorageType,
    attributes: FileAttributes,
    tags: Tags,
    rev: Int,
    deprecated: Boolean,
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends ScopedState {

  /**
    * @return
    *   the schema reference that storages conforms to
    */
  def schema: ResourceRef = Latest(schemas.files)

  /**
    * @return
    *   the collection of known types of file resources
    */
  def types: Set[Iri] = Set(nxvFile)

  private def file: File = File(id, project, storage, storageType, attributes, tags)

  def toResource: FileResource =
    ResourceF(
      id = id,
      access = ResourceAccess("files", project, id),
      rev = rev,
      types = types,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      schema = schema,
      value = file
    )
}

object FileState {

  implicit val serializer: Serializer[Iri, FileState] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration                        = Serializer.circeConfiguration.withDefaults
    implicit val digestCodec: Codec.AsObject[Digest]                 =
      deriveConfiguredCodec[Digest]
    implicit val fileAttributesCodec: Codec.AsObject[FileAttributes] = {
      val enc = FileAttributes.createConfiguredEncoder(configuration)

      Codec.AsObject.from(deriveConfiguredDecoder[FileAttributes], enc)
    }

    implicit val codec: Codec.AsObject[FileState] = deriveConfiguredCodec[FileState]
    Serializer.dropNullsInjectType()
  }
}
