package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object typehierarchy {

  object contexts {
    val definition = Set(Vocabulary.contexts.typeHierarchy -> "contexts/type-hierarchy.json")
  }

}
