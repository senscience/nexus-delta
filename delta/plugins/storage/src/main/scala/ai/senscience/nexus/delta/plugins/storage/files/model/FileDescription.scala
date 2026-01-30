package ai.senscience.nexus.delta.plugins.storage.files.model

import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

case class FileDescription(
    filename: String,
    mediaType: Option[MediaType],
    metadata: Option[FileCustomMetadata]
)

object FileDescription {

  private given Configuration  = Configuration.default
  given Codec[FileDescription] = deriveConfiguredCodec[FileDescription]

}
