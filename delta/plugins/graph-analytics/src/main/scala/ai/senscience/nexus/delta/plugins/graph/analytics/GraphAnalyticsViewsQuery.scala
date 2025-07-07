package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.plugins.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.sdk.model.search.SortList
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import io.circe.{Json, JsonObject}
import org.http4s.Query

/** Allows to perform elasticsearch queries on Graph Analytics views */
trait GraphAnalyticsViewsQuery {

  /**
    * In a given project, perform the provided elasticsearch query on the projects' Graph Analytics view.
    * @param projectRef
    *   project in which to make the query
    * @param query
    *   elasticsearch query to perform on the Graph Analytics view
    * @param qp
    *   the extra query parameters for the elasticsearch index
    */
  def query(projectRef: ProjectRef, query: JsonObject, qp: Query): IO[Json]
}

/**
  * A [[GraphAnalyticsViewsQuery]] implementation that uses the [[ElasticSearchClient]] to query views.
  * @param prefix
  *   prefix used in the names of the elasticsearch indices
  * @param client
  *   elasticsearch client
  */
class GraphAnalyticsViewsQueryImpl(prefix: String, client: ElasticSearchClient) extends GraphAnalyticsViewsQuery {
  override def query(projectRef: ProjectRef, query: JsonObject, qp: Query): IO[Json] = {
    val index = GraphAnalytics.index(prefix, projectRef)
    client.search(query, Set(index.value), qp)(SortList.empty)
  }

}
