package ai.senscience.nexus.delta.plugins.storage.files

import ai.senscience.nexus.delta.kernel.error.NotARejection
import ai.senscience.nexus.delta.plugins.storage.files.model.FileRejection.{FileTooLarge, FileUnmarshallingRejection, InvalidMultipartFieldName}
import ai.senscience.nexus.delta.sdk.stream.StreamConverter
import cats.effect.IO
import cats.syntax.all.*
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.MediaTypes.`multipart/form-data`
import org.apache.pekko.http.scaladsl.model.Multipart.FormData
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller.UnsupportedContentTypeException
import org.apache.pekko.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, MultipartUnmarshallers, Unmarshaller}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{Graph, SourceShape}
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

trait FormDataExtractor {

  /**
    * Extracts the part with fieldName ''file'' from the passed ''entity'' MultiPart/FormData. Any other part is
    * discarded.
    *
    * @param entity
    *   the Multipart/FormData payload
    * @param maxFileSize
    *   the file size limit to be uploaded, provided by the storage
    * @return
    *   the file metadata. plus the entity with the file content
    */
  def apply(entity: HttpEntity, maxFileSize: Long): IO[UploadedFileInformation]
}

final case class UploadedFileInformation(
    filename: String,
    mediaType: Option[model.MediaType],
    contents: FileData
)

object FormDataExtractor {

  private val FileFieldName: String   = "file"
  private val defaultFilename: String = "file"

  private val defaultContentType: ContentType.Binary = ContentTypes.`application/octet-stream`

  // Creating an unmarshaller defaulting to `application/octet-stream` as a content type
  @SuppressWarnings(Array("TryGet"))
  private given um: FromEntityUnmarshaller[Multipart.FormData] =
    MultipartUnmarshallers
      .multipartUnmarshaller[Multipart.FormData, Multipart.FormData.BodyPart, Multipart.FormData.BodyPart.Strict](
        mediaRange = `multipart/form-data`,
        defaultContentType = ContentTypes.`application/octet-stream`,
        createBodyPart = (entity, headers) => Multipart.General.BodyPart(entity, headers).toFormDataBodyPart.get,
        createStreamed = (_, parts) => Multipart.FormData(parts),
        createStrictBodyPart =
          (entity, headers) => Multipart.General.BodyPart.Strict(entity, headers).toFormDataBodyPart.get,
        createStrict = (_, parts) => Multipart.FormData.Strict(parts)
      )

  def apply(mediaTypeDetector: MediaTypeDetector)(using as: ActorSystem): FormDataExtractor =
    new FormDataExtractor {
      given ExecutionContext = as.getDispatcher

      override def apply(entity: HttpEntity, maxFileSize: Long): IO[UploadedFileInformation] = {
        for {
          formData <- unmarshall(entity, maxFileSize)
          fileOpt  <- extractFile(formData, maxFileSize)
          file     <- IO.fromOption(fileOpt)(InvalidMultipartFieldName)
        } yield file
      }

      private def unmarshall(entity: HttpEntity, sizeLimit: Long): IO[FormData] =
        IO.fromFuture(IO.delay(um(entity.withSizeLimit(sizeLimit)))).adaptError(onUnmarshallingError(_))

      private def onUnmarshallingError(th: Throwable): FileUnmarshallingRejection = th match {
        case RejectionError(r)                  =>
          FileUnmarshallingRejection(r)
        case Unmarshaller.NoContentException    =>
          FileUnmarshallingRejection(RequestEntityExpectedRejection)
        case x: UnsupportedContentTypeException =>
          FileUnmarshallingRejection(UnsupportedRequestContentTypeRejection(x.supported, x.actualContentType))
        case x: IllegalArgumentException        =>
          FileUnmarshallingRejection(ValidationRejection(Option(x.getMessage).getOrElse(""), Some(x)))
        case x: ExceptionWithErrorInfo          =>
          FileUnmarshallingRejection(MalformedRequestContentRejection(x.info.format(withDetail = false), x))
        case x                                  =>
          FileUnmarshallingRejection(MalformedRequestContentRejection(Option(x.getMessage).getOrElse(""), x))
      }

      private def extractFile(
          formData: FormData,
          maxFileSize: Long
      ): IO[Option[UploadedFileInformation]] = IO
        .fromFuture(
          IO(
            formData.parts
              .mapAsync(parallelism = 1)(extractFile)
              .collect { case Some(values) => values }
              .toMat(Sink.headOption)(Keep.right)
              .run()
          )
        )
        .adaptError {
          case _: EntityStreamSizeException =>
            FileTooLarge(maxFileSize)
          case NotARejection(th)            =>
            FileUnmarshallingRejection(MalformedRequestContentRejection(th.getMessage, th))
        }

      private def extractFile(part: FormData.BodyPart): Future[Option[UploadedFileInformation]] = part match {
        case part if part.name == FileFieldName =>
          val filename               = part.filename.filterNot(_.isEmpty).getOrElse(defaultFilename)
          val contentTypeFromRequest = part.entity.contentType
          val suppliedContentType    = Option.when(contentTypeFromRequest != defaultContentType)(contentTypeFromRequest)

          val detectedMediaType = mediaTypeDetector(
            filename,
            suppliedContentType.flatMap(toHttp4sMediaType),
            Some(contentTypeFromRequest).flatMap(toHttp4sMediaType)
          )

          Future(
            UploadedFileInformation(
              filename,
              detectedMediaType,
              convertStream(part.entity.dataBytes)
            ).some
          )
        case part                               =>
          part.entity.discardBytes().future.as(None)
      }

      private def convertStream(source: Source[ByteString, Any]) =
        StreamConverter(source.asInstanceOf[Graph[SourceShape[ByteString], NotUsed]]).map { byteString =>
          byteString.asByteBuffer
        }

      private def toHttp4sMediaType(contentType: ContentType) =
        model.MediaType.parse(contentType.value).toOption
    }
}
