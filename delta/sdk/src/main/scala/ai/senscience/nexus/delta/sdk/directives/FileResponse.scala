package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.directives.FileResponse.{Content, Metadata}
import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.stream.StreamConverter
import ai.senscience.nexus.delta.sdk.{FileData, JsonLdValue}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.MediaType.NotCompressible
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import cats.effect.IO
import cats.syntax.all.*

import java.util.Locale
import scala.reflect.ClassTag

/**
  * A file response content
  *
  * @param metadata
  *   the file metadata
  * @param content
  *   the file content
  */
final case class FileResponse private (metadata: Metadata, content: Content)

object FileResponse {

  type PekkoSource = Source[ByteString, Any]
  type Content     = IO[Either[Complete[JsonLdValue], PekkoSource]]

  /**
    * Metadata for the file response
    *
    * @param filename
    *   the filename
    * @param contentType
    *   the file content type
    * @param bytes
    *   the file size
    */
  final case class Metadata(
      filename: String,
      contentType: ContentType,
      etag: Option[String],
      bytes: Option[Long]
  )

  object Metadata {
    implicit def fileResponseMetadataHttpResponseFields: HttpResponseFields[Metadata] =
      new HttpResponseFields[Metadata] {
        override def statusFrom(value: Metadata): StatusCode       = StatusCodes.OK
        override def headersFrom(value: Metadata): Seq[HttpHeader] = Seq.empty

        override def entityTag(value: Metadata): Option[String] = value.etag
      }
  }

  def apply[E <: Throwable: ClassTag: JsonLdEncoder: HttpResponseFields](
      filename: String,
      contentType: ContentType,
      etag: Option[String],
      bytes: Option[Long],
      data: FileData
  ): FileResponse = {
    new FileResponse(
      Metadata(
        filename,
        markBinaryAsNonCompressible(contentType),
        etag,
        bytes
      ),
      convertStream(data).attemptNarrow[E].map { r =>
        r.leftMap { e =>
          Complete(e).map(JsonLdValue(_))
        }
      }
    )
  }

  /**
    * When parsing a custom binary media type, pekko assumes that it is compressible which is traduced by a performance
    * hit when we compress responses so we revert this
    */
  private[directives] def markBinaryAsNonCompressible(contentType: ContentType) =
    contentType match {
      case b: ContentType.Binary if isCustomMediaType(b.mediaType) =>
        ContentType.Binary(b.mediaType.withComp(NotCompressible))
      case other                                                   => other
    }

  private def isCustomMediaType(mediaType: MediaType) =
    MediaTypes
      .getForKey(mediaType.mainType.toLowerCase(Locale.ROOT) -> mediaType.subType.toLowerCase(Locale.ROOT))
      .isEmpty

  def unsafe(
      filename: String,
      contentType: ContentType,
      etag: Option[String],
      bytes: Option[Long],
      data: FileData
  ): FileResponse =
    new FileResponse(Metadata(filename, contentType, etag, bytes), convertStream(data).map(Right(_)))

  def noCache(filename: String, contentType: ContentType, bytes: Option[Long], source: PekkoSource): FileResponse =
    new FileResponse(Metadata(filename, contentType, None, bytes), IO.pure(Right(source)))

  private def convertStream(data: FileData) =
    IO.delay {
      StreamConverter(data)
        .map(bytes => ByteString(bytes))
    }
}
