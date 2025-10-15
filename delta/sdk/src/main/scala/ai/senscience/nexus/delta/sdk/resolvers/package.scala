package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object resolvers {

  object contexts {
    val definition = Set(
      Vocabulary.contexts.resolvers         -> "contexts/resolvers.json",
      Vocabulary.contexts.resolversMetadata -> "contexts/resolvers-metadata.json"
    )
  }

}
