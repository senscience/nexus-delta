package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.plugins.storage.files.Files
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.ShowFileLocation
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageType
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.ResourceShift
import ai.senscience.nexus.delta.sdk.indexing.{MainDocument, MainDocumentEncoder}
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef, Tags}
import cats.syntax.all.*
import io.circe.generic.extras.Configuration
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json}

/**
  * A representation of a file information
  *
  * @param id
  *   the file identifier
  * @param project
  *   the project where the file belongs
  * @param storage
  *   the reference to the used storage
  * @param storageType
  *   the type of storage
  * @param attributes
  *   the file attributes
  * @param tags
  *   the file tags
  */
final case class File(
    id: Iri,
    project: ProjectRef,
    storage: ResourceRef.Revision,
    storageType: StorageType,
    attributes: FileAttributes,
    tags: Tags
)

object File {

  implicit def fileEncoder(implicit showLocation: ShowFileLocation): Encoder.AsObject[File] =
    Encoder.encodeJsonObject.contramapObject { file =>
      val storageType: StorageType                      = file.storageType
      val storageJson                                   = Json.obj(
        keywords.id  -> file.storage.iri.asJson,
        keywords.tpe -> storageType.iri.asJson,
        "_rev"       -> file.storage.rev.asJson
      )
      val attrEncoder: Encoder.AsObject[FileAttributes] = FileAttributes.createConfiguredEncoder(
        Configuration.default,
        underscoreFieldsForMetadata = true,
        removePath = true,
        removeLocation = !showLocation.types.contains(storageType)
      )
      attrEncoder.encodeObject(file.attributes).add("_storage", storageJson)
    }

  implicit def fileJsonLdEncoder(implicit showLocation: ShowFileLocation): JsonLdEncoder[File] =
    JsonLdEncoder.computeFromCirce(_.id, Files.context)

  type Shift = ResourceShift[FileState, File]

  def shift(files: Files)(implicit baseUri: BaseUri, showLocation: ShowFileLocation): Shift =
    ResourceShift[FileState, File](
      Files.entityType,
      (ref, project) => files.fetch(FileId(ref, project)),
      state => state.toResource,
      value => JsonLdContent(value, value.value.asJson, value.value.tags)
    )

  def mainDocumentEncoder(using BaseUri, ShowFileLocation): MainDocumentEncoder[FileState, File] =
    new MainDocumentEncoder[FileState, File] {
      override def entityType: EntityType = Files.entityType

      override def databaseDecoder: Decoder[FileState] = FileState.serializer.codec

      protected def toResourceF(state: FileState): ResourceF[File] = state.toResource

      override def fromResource(value: ResourceF[File]): MainDocument =
        MainDocument(
          name = value.value.attributes.name,
          label = None,
          prefLabel = None,
          description = value.value.attributes.description,
          metadata = value.void,
          tags = value.value.tags,
          originalSource = value.value.asJson,
          additionalFields = fileEncoder.encodeObject(value.value)
        )
    }

}
