package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object permissions {
  object contexts {
    val definition = Set(
      Vocabulary.contexts.permissions         -> "contexts/permissions.json",
      Vocabulary.contexts.permissionsMetadata -> "contexts/permissions-metadata.json"
    )
  }
}
