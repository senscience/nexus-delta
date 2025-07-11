package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri

/**
  * Defines the context applied within a project
  * @param apiMappings
  *   the API mappings
  * @param base
  *   the base Iri for generated resource IDs
  * @param vocab
  *   an optional vocabulary for resources with no context
  * @param enforceSchema
  *   to ban unconstrained resources in this project
  */
final case class ProjectContext(apiMappings: ApiMappings, base: ProjectBase, vocab: Iri, enforceSchema: Boolean)

object ProjectContext {

  def unsafe(apiMappings: ApiMappings, base: Iri, vocab: Iri, enforceSchema: Boolean): ProjectContext =
    ProjectContext(apiMappings, ProjectBase(base), vocab, enforceSchema)
}
