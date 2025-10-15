package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object acls {

  object contexts {
    val definition = Set(
      Vocabulary.contexts.acls         -> "contexts/acls.json",
      Vocabulary.contexts.aclsMetadata -> "contexts/acls-metadata.json"
    )
  }

}
