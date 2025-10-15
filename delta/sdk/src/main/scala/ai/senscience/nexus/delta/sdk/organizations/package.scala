package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object organizations {

  object contexts {
    val definition = Set(
      Vocabulary.contexts.organizations         -> "contexts/organizations.json",
      Vocabulary.contexts.organizationsMetadata -> "contexts/organizations-metadata.json"
    )
  }

}
