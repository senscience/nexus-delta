package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.sdk.implicits.*
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import org.http4s.Uri.Path

final case class FileLinkRequest(path: Path, mediaType: Option[MediaType], metadata: Option[FileCustomMetadata])

object FileLinkRequest {
  implicit private val config: Configuration                    = Configuration.default
  implicit val linkFileRequestDecoder: Decoder[FileLinkRequest] = deriveConfiguredDecoder[FileLinkRequest]
}
