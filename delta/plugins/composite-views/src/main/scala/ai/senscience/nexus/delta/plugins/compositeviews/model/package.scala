package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts as nxvContexts, nxv, schemas}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.ResourceRef
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest

package object model {
  final val schema: ResourceRef = Latest(schemas + "views.json")
  final val compositeViewType   = nxv + "CompositeView"
  type ViewResource = ResourceF[CompositeView]

  /**
    * Composite views contexts.
    */
  object contexts {
    val compositeViews         = nxvContexts + "composite-views.json"
    val compositeViewsMetadata = nxvContexts + "composite-views-metadata.json"
  }

  object permissions {
    val write: Permission = Permission.unsafe("views/write")
    val read: Permission  = Permissions.resources.read
    val query: Permission = Permission.unsafe("views/query")
  }
}
