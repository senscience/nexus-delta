package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object identities {

  object contexts {
    val definition = Set(
      Vocabulary.contexts.identities -> "contexts/identities.json"
    )
  }

}
