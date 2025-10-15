package ai.senscience.nexus.delta.plugins.graph

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts as nxvContexts
import ai.senscience.nexus.delta.sdk.permissions.model.Permission

package object analytics {
  object contexts {
    val relationships: Iri = nxvContexts + "relationships.json"
    val properties: Iri    = nxvContexts + "properties.json"

    val definition = Set(
      contexts.relationships -> "contexts/relationships.json",
      contexts.properties    -> "contexts/properties.json"
    )
  }

  object permissions {
    final val query: Permission = Permission.unsafe("views/query")
  }
}
