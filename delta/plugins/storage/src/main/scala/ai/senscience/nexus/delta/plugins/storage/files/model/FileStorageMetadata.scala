package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.plugins.storage.files.model.FileAttributes.FileAttributesOrigin
import org.http4s.Uri
import org.http4s.Uri.Path

import java.util.UUID

case class FileStorageMetadata(
    uuid: UUID,
    bytes: Long,
    digest: Digest,
    origin: FileAttributesOrigin,
    location: Uri,
    path: Path
)
