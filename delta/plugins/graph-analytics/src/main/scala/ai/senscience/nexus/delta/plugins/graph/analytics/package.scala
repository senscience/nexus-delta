package ai.senscience.nexus.delta.plugins.graph

import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import ch.epfl.bluebrain.nexus.delta.rdf.Vocabulary.contexts as nxvContexts

package object analytics {
  object contexts {
    val relationships: Iri = nxvContexts + "relationships.json"
    val properties: Iri    = nxvContexts + "properties.json"
  }

  object permissions {
    final val query: Permission = Permission.unsafe("views/query")
  }
}
