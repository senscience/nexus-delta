package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.sdk.implicits.given
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import org.http4s.Uri.Path

final case class FileLinkRequest(path: Path, mediaType: Option[MediaType], metadata: Option[FileCustomMetadata])

object FileLinkRequest {
  given Configuration            = Configuration.default
  given Decoder[FileLinkRequest] = deriveConfiguredDecoder[FileLinkRequest]
}
