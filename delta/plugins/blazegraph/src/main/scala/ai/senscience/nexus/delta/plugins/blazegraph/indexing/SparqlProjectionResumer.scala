package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.sourcing.stream.{ProjectionActivations, ProjectionResumer}

type SparqlProjectionResumer = ProjectionResumer[ActiveViewDef]

object SparqlProjectionResumer {

  /**
    * The resumer that resumes Sparql views as activations come in. It fetches the authoritative current views from the
    * store (not the cache), so it never depends on the coordinator having recorded them yet.
    */
  def apply(
      currentActiveViews: CurrentActiveViews,
      activations: ProjectionActivations
  ): SparqlProjectionResumer =
    ProjectionResumer(
      BlazegraphViews.entityType.value,
      currentActiveViews.stream,
      currentActiveViews.fetch,
      activations
    )

}
