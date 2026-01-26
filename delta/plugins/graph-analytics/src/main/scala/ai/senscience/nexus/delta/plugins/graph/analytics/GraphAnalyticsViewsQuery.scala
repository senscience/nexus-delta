package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, ElasticSearchRequest}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import io.circe.Json

/** Allows to perform elasticsearch queries on Graph Analytics views */
trait GraphAnalyticsViewsQuery {

  /**
    * In a given project, perform the provided elasticsearch query on the projects' Graph Analytics view.
    * @param projectRef
    *   project in which to make the query
    * @param request
    *   elasticsearch request to perform on the Graph Analytics view
    */
  def query(projectRef: ProjectRef, request: ElasticSearchRequest): IO[Json]
}

/**
  * A [[GraphAnalyticsViewsQuery]] implementation that uses the [[ElasticSearchClient]] to query views.
  * @param prefix
  *   prefix used in the names of the elasticsearch indices
  * @param client
  *   elasticsearch client
  */
class GraphAnalyticsViewsQueryImpl(prefix: String, client: ElasticSearchClient) extends GraphAnalyticsViewsQuery {
  override def query(projectRef: ProjectRef, request: ElasticSearchRequest): IO[Json] = {
    val index = GraphAnalytics.index(prefix, projectRef)
    client.search(request, Set(index.value))
  }

}
