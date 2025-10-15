package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object schemas {

  object contexts {
    val definition = Set(
      Vocabulary.contexts.shacl           -> "contexts/shacl.json",
      Vocabulary.contexts.schemasMetadata -> "contexts/schemas-metadata.json"
    )
  }

}
