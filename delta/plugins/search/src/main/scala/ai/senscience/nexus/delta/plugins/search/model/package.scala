package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings

package object model {

  val defaultViewId: Iri       = nxv + "searchView"
  val defaultSourceId: Iri     = nxv + "searchSource"
  val defaultProjectionId: Iri = nxv + "searchProjection"

  /**
    * The default Search API mappings
    */
  val defaulMappings: ApiMappings = ApiMappings("search" -> defaultViewId)
}
