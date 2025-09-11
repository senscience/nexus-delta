package ai.senscience.nexus.delta.plugins.archive.model

import ai.senscience.nexus.delta.sdk.utils.HeadersUtils
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.{ContentType, MediaTypes}
import org.apache.pekko.http.scaladsl.server.Directive
import org.apache.pekko.http.scaladsl.server.Directives.extractRequest
import org.apache.pekko.stream.connectors.file.ArchiveMetadata
import org.apache.pekko.stream.connectors.file.scaladsl.Archive
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util.ByteString

/**
  * Zip archive format
  *
  * @see
  *   https://en.wikipedia.org/wiki/ZIP_(file_format)#Limits for the limitations
  */
object Zip {
  type WriteFlow[Metadata] = Flow[(Metadata, Source[ByteString, ?]), ByteString, NotUsed]

  lazy val contentType: ContentType = MediaTypes.`application/zip`

  lazy val writeFlow: WriteFlow[ArchiveMetadata] = Archive.zip()

  lazy val ordering: Ordering[ArchiveMetadata] = Ordering.by(md => md.filePath)

  def metadata(filename: String): ArchiveMetadata = ArchiveMetadata.create(filename)

  def checkHeader: Directive[Tuple1[Boolean]] =
    extractRequest.map { req =>
      HeadersUtils.matches(req.headers, Zip.contentType.mediaType)
    }

}
