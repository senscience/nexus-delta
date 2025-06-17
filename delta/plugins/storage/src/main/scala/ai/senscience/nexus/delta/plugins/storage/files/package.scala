package ai.senscience.nexus.delta.plugins.storage

import ai.senscience.nexus.delta.plugins.storage.files.model.File
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, contexts as nxvContexts, schemas as nxvSchema}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import cats.effect.IO
import fs2.Stream

import java.nio.ByteBuffer

package object files {

  /**
    * Type alias for a file specific resource.
    */
  type FileResource = ResourceF[File]

  type FileData = Stream[IO, ByteBuffer]

  /**
    * File schemas
    */
  object schemas {
    val files: Iri = nxvSchema + "files.json"
  }

  /**
    * File vocabulary
    */
  val nxvFile: Iri = nxv + "File"

  object permissions {
    final val read: Permission  = resources.read
    final val write: Permission = Permission.unsafe("files/write")
  }

  /**
    * File contexts
    */
  object contexts {
    val files: Iri = nxvContexts + "files.json"
  }
}
