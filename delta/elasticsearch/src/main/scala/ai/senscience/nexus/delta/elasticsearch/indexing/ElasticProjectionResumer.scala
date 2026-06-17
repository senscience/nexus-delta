package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.sourcing.stream.{ProjectActivity, ProjectionActivations, ProjectionResumer}

type ElasticProjectionResumer = ProjectionResumer[ActiveViewDef]

object ElasticProjectionResumer {

  /**
    * The resumer that resumes Elasticsearch views as activations come in. It fetches the authoritative current views
    * from the store (not the cache), so it never depends on the coordinator having recorded them yet.
    */
  def apply(
      currentActiveViews: CurrentActiveViews,
      projectActivity: ProjectActivity,
      activations: ProjectionActivations
  ): ElasticProjectionResumer =
    ProjectionResumer(
      ElasticSearchViews.entityType.value,
      currentActiveViews.stream,
      currentActiveViews.fetch,
      projectActivity,
      activations
    )

}
