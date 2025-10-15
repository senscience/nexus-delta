package ai.senscience.nexus.delta.plugins

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts as nxvContexts

package object search {

  object contexts {
    val suites     = nxvContexts + "suites.json"
    val definition = Set(suites -> "contexts/suites.json")
  }

}
