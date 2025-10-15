package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object realms {

  object contexts {
    val definition = Set(
      Vocabulary.contexts.realms         -> "contexts/realms.json",
      Vocabulary.contexts.realmsMetadata -> "contexts/realms-metadata.json"
    )
  }

}
