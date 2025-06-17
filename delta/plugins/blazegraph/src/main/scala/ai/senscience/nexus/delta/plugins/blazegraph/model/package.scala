package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.ResourceRef
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import ch.epfl.bluebrain.nexus.delta.rdf.Vocabulary.{contexts as nxvContexts, nxv, schemas}

package object model {

  /**
    * Type alias for a view specific resource.
    */
  type ViewResource = ResourceF[BlazegraphView]

  /**
    * The fixed virtual schema of a BlazegraphView.
    */
  final val schema: ResourceRef = Latest(schemas + "views.json")

  /**
    * Blazegraph views contexts.
    */
  object contexts {
    val blazegraph: Iri         = nxvContexts + "sparql.json"
    val blazegraphMetadata: Iri = nxvContexts + "sparql-metadata.json"
  }

  object permissions {
    final val read: Permission  = resources.read
    final val write: Permission = Permission.unsafe("views/write")
    final val query: Permission = Permission.unsafe("views/query")
  }

  /**
    * The id for the default blazegraph view
    */
  final val defaultViewId = nxv + "defaultSparqlIndex"
}
