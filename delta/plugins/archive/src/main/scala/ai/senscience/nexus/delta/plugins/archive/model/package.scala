package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts as nxvContexts, nxv, schemas}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.ResourceRef
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest

package object model {

  /**
    * Type alias for an archive resource.
    */
  type ArchiveResource = ResourceF[Archive]

  /**
    * The fixed virtual schema of an Archive.
    */
  final val schema: ResourceRef = Latest(schemas + "archives.json")

  /**
    * The archive type.
    */
  final val tpe: Iri = nxv + "Archive"

  /**
    * Archive contexts.
    */
  object contexts {
    final val archives: Iri         = nxvContexts + "archives.json"
    final val archivesMetadata: Iri = nxvContexts + "archives-metadata.json"
  }

  /**
    * Archive permissions.
    */
  object permissions {
    final val read: Permission  = Permissions.resources.read
    final val write: Permission = Permissions.resources.read
  }
}
