package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.Vocabulary

package object projects {

  object contexts {
    val definition = Set(
      Vocabulary.contexts.projects         -> "contexts/projects.json",
      Vocabulary.contexts.projectsMetadata -> "contexts/projects-metadata.json"
    )
  }

}
