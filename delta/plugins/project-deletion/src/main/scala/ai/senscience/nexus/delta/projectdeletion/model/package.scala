package ai.senscience.nexus.delta.projectdeletion

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts as nxvContexts

package object model {
  object contexts {
    val projectDeletion: Iri = nxvContexts + "project-deletion.json"

    val definition = Set(projectDeletion -> "contexts/project-deletion.json")
  }
}
