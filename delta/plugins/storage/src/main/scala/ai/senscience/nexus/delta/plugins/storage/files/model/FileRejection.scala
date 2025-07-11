package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.plugins.storage.storages.operations.StorageFileRejection
import ai.senscience.nexus.delta.plugins.storage.storages.operations.StorageFileRejection.{FetchFileRejection, SaveFileRejection}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.marshalling.RdfRejectionHandler.all.*
import ai.senscience.nexus.delta.sdk.syntax.httpResponseFieldsSyntax
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Rejection as AkkaRejection
import io.circe.syntax.*
import io.circe.{Encoder, JsonObject}

/**
  * Enumeration of File rejection types.
  *
  * @param reason
  *   a descriptive message as to why the rejection occurred
  */
sealed abstract class FileRejection(val reason: String, val loggedDetails: Option[String] = None) extends Rejection

object FileRejection {

  /**
    * Rejection returned when a subject intends to retrieve a file at a specific revision, but the provided revision
    * does not exist.
    *
    * @param provided
    *   the provided revision
    * @param current
    *   the last known revision
    */
  final case class RevisionNotFound(provided: Int, current: Int)
      extends FileRejection(s"Revision requested '$provided' not found, last known revision is '$current'.")

  /**
    * Rejection returned when a subject intends to retrieve a file at a specific tag, but the provided tag does not
    * exist.
    *
    * @param tag
    *   the provided tag
    */
  final case class TagNotFound(tag: UserTag) extends FileRejection(s"Tag requested '$tag' not found.")

  /**
    * Rejection returned when attempting to create a file but the id already exists.
    *
    * @param id
    *   the resource identifier
    * @param project
    *   the project it belongs to
    */
  final case class ResourceAlreadyExists(id: Iri, project: ProjectRef)
      extends FileRejection(s"Resource '$id' already exists in project '$project'.")

  /**
    * Rejection returned when attempting to update a file with an id that doesn't exist.
    *
    * @param id
    *   the file identifier
    * @param project
    *   the project it belongs to
    */
  final case class FileNotFound(id: Iri, project: ProjectRef)
      extends FileRejection(s"File '$id' not found in project '$project'.")

  /**
    * Rejection returned when attempting to interact with a file providing an id that cannot be resolved to an Iri.
    *
    * @param id
    *   the file identifier
    */
  final case class InvalidFileId(id: String)
      extends FileRejection(s"File identifier '$id' cannot be expanded to an Iri.")

  /**
    * Signals the impossibility to update a file when the digest is not computed
    *
    * @param id
    *   the file identifier
    */
  final case class DigestNotComputed(id: Iri)
      extends FileRejection(
        s"The digest computation for the current file '$id' is not yet complete; the file cannot be updated"
      )

  /**
    * Rejection returned when a subject intends to perform an operation on the current file, but either provided an
    * incorrect revision or a concurrent update won over this attempt.
    *
    * @param provided
    *   the provided revision
    * @param expected
    *   the expected revision
    */
  final case class IncorrectRev(provided: Int, expected: Int)
      extends FileRejection(
        s"Incorrect revision '$provided' provided, expected '$expected', the file may have been updated since last seen."
      )

  /**
    * Rejection returned when attempting to update/deprecate a file that is already deprecated.
    *
    * @param id
    *   the file identifier
    */
  final case class FileIsDeprecated(id: Iri) extends FileRejection(s"File '$id' is deprecated.")

  /**
    * Rejection returned when attempting to undeprecate a file that is already deprecated.
    *
    * @param id
    *   the file identifier
    */
  final case class FileIsNotDeprecated(id: Iri) extends FileRejection(s"File '$id' is not deprecated.")

  /**
    * Rejection returned when attempting to create/update a file with a Multipart/Form-Data payload that does not
    * contain a ''file'' fieldName
    */
  final case object InvalidMultipartFieldName
      extends FileRejection(s"Multipart/Form-Data payload does not contain a 'file' part.")

  final case object EmptyCustomMetadata extends FileRejection(s"No metadata was provided")

  /**
    * Rejection returned when attempting to create/update a file with a Multipart/Form-Data payload that does not
    * contain a ''file'' fieldName
    */
  final case class FileTooLarge(maxFileSize: Long)
      extends FileRejection(s"File size exceeds the max file size for the storage ($maxFileSize bytes).")

  /**
    * Rejection returned when attempting to create/update a file and the unmarshaller fails
    */
  final case class FileUnmarshallingRejection(rejection: AkkaRejection) extends FileRejection(rejection.toString)

  /**
    * Rejection returned when interacting with the storage operations bundle to fetch a file from a storage
    *
    * @param id
    *   the file id
    * @param storageId
    *   the storage id
    * @param rejection
    *   the rejection which occurred with the storage
    */
  final case class FetchRejection(id: Iri, storageId: Iri, rejection: StorageFileRejection.FetchFileRejection)
      extends FileRejection(
        s"File '$id' could not be fetched using storage '$storageId'",
        Some(rejection.loggedDetails)
      )

  /**
    * Rejection returned when interacting with the storage operations bundle to save a file in a storage
    *
    * @param id
    *   the file id
    * @param storageId
    *   the storage id
    * @param rejection
    *   the rejection which occurred with the storage
    */
  final case class SaveRejection(id: Iri, storageId: Iri, rejection: StorageFileRejection.SaveFileRejection)
      extends FileRejection(s"File '$id' could not be saved using storage '$storageId'", Some(rejection.loggedDetails))

  /**
    * Rejection returned when attempting to link a file without providing a filename or a path that ends with a
    * filename.
    */
  final case object InvalidFilePath
      extends FileRejection(
        s"Linking a file cannot be performed without a 'filename' or a 'path' that does not end with a filename."
      )

  implicit val fileRejectionEncoder: Encoder.AsObject[FileRejection] =
    Encoder.AsObject.instance { r =>
      val tpe = ClassUtils.simpleName(r)
      val obj = JsonObject(keywords.tpe -> tpe.asJson, "reason" -> r.reason.asJson)
      r match {
        case FileUnmarshallingRejection(rejection) => rejection.asJsonObject
        case SaveRejection(_, _, rejection)        =>
          obj.add(keywords.tpe, ClassUtils.simpleName(rejection).asJson).add("details", rejection.loggedDetails.asJson)
        case FetchRejection(_, _, rejection)       =>
          obj.add(keywords.tpe, ClassUtils.simpleName(rejection).asJson).add("details", rejection.loggedDetails.asJson)
        case IncorrectRev(provided, expected)      => obj.add("provided", provided.asJson).add("expected", expected.asJson)
        case _: FileNotFound                       => obj.add(keywords.tpe, "ResourceNotFound".asJson)
        case _                                     => obj
      }
    }

  implicit final val fileRejectionJsonLdEncoder: JsonLdEncoder[FileRejection] =
    JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.error))

  implicit final val fileRejectionHttpResponseFields: HttpResponseFields[FileRejection] =
    HttpResponseFields.fromStatusAndHeaders {
      case RevisionNotFound(_, _)                                             => (StatusCodes.NotFound, Seq.empty)
      case TagNotFound(_)                                                     => (StatusCodes.NotFound, Seq.empty)
      case FileNotFound(_, _)                                                 => (StatusCodes.NotFound, Seq.empty)
      case ResourceAlreadyExists(_, _)                                        => (StatusCodes.Conflict, Seq.empty)
      case IncorrectRev(_, _)                                                 => (StatusCodes.Conflict, Seq.empty)
      case FileTooLarge(_)                                                    => (StatusCodes.PayloadTooLarge, Seq.empty)
      case FileUnmarshallingRejection(rej)                                    => (rej.status, rej.headers)
      // If this happens it signifies a system problem rather than the user having made a mistake
      case FetchRejection(_, _, FetchFileRejection.FileNotFound(_))           => (StatusCodes.InternalServerError, Seq.empty)
      case SaveRejection(_, _, SaveFileRejection.ResourceAlreadyExists(_))    => (StatusCodes.Conflict, Seq.empty)
      case SaveRejection(_, _, SaveFileRejection.BucketAccessDenied(_, _, _)) => (StatusCodes.Forbidden, Seq.empty)
      case SaveRejection(_, _, SaveFileRejection.FileContentLengthIsMissing)  => (StatusCodes.BadRequest, Seq.empty)
      case FetchRejection(_, _, _)                                            => (StatusCodes.InternalServerError, Seq.empty)
      case SaveRejection(_, _, _)                                             => (StatusCodes.InternalServerError, Seq.empty)
      case _                                                                  => (StatusCodes.BadRequest, Seq.empty)
    }
}
