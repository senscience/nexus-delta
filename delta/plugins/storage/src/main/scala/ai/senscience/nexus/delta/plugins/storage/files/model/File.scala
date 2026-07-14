package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.plugins.storage.files.Files
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.ShowFileLocation
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageType
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.ResourceShift
import ai.senscience.nexus.delta.sdk.implicits.given
import ai.senscience.nexus.delta.sdk.indexing.{MainDocument, MainDocumentEncoder}
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef, Tags}
import cats.syntax.all.*
import io.circe.generic.extras.Configuration
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json, JsonObject}

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

  private def encodeStorage(file: File) =
    Json.obj(
      keywords.id  := file.storage.iri,
      keywords.tpe := file.storageType.iri,
      "_rev"       := file.storage.rev
    )

  given fileEncoder: (showLocation: ShowFileLocation) => Encoder.AsObject[File] =
    Encoder.encodeJsonObject.contramapObject { file =>
      val attrEncoder: Encoder.AsObject[FileAttributes] = FileAttributes.createConfiguredEncoder(
        Configuration.default,
        underscoreFieldsForMetadata = true,
        removePath = true,
        removeLocation = !showLocation.types.contains(file.storageType)
      )
      attrEncoder.encodeObject(file.attributes).add("_storage", encodeStorage(file))
    }

  given ShowFileLocation => JsonLdEncoder[File] =
    JsonLdEncoder.computeFromCirce(_.id, Files.context)

  type Shift = ResourceShift[FileState, File]

  def shift(files: Files)(using BaseUri, ShowFileLocation): Shift =
    ResourceShift[FileState, File](
      Files.entityType,
      (ref, project) => files.fetch(FileId(ref, project)),
      state => state.toResource,
      value => JsonLdContent(value, value.value.asJson, value.value.tags)
    )

  def mainDocumentEncoder(using base: BaseUri, showLocation: ShowFileLocation): MainDocumentEncoder[FileState, File] =
    new MainDocumentEncoder[FileState, File] {
      override def entityType: EntityType = Files.entityType

      override def databaseDecoder: Decoder[FileState] = FileState.serializer.codec

      protected def toResourceF(state: FileState): ResourceF[File] = state.toResource

      override def fromResource(value: ResourceF[File]): MainDocument = {
        val file          = value.value
        val attributes    = file.attributes
        val allowLocation = !showLocation.types.contains(file.storageType)
        MainDocument(
          name = attributes.name,
          label = None,
          prefLabel = None,
          description = attributes.description,
          keywords = attributes.keywords,
          metadata = value.void,
          tags = file.tags,
          originalSource = file.asJson,
          additionalMetadata = JsonObject(
            "_uuid"      := attributes.uuid,
            "_storage"   := encodeStorage(file),
            "_bytes"     := attributes.bytes,
            "_mediaType" := attributes.mediaType,
            "_origin"    := attributes.origin,
            "_location"  := Option.when(allowLocation)(attributes.location),
            "_filename"  := attributes.filename,
            "_digest"    := attributes.digest
          )
        )
      }
    }

}
