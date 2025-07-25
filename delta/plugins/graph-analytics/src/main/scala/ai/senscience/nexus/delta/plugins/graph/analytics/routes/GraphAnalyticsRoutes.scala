package ai.senscience.nexus.delta.plugins.graph.analytics.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchExceptionHandler
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchViewsDirectives.extractQueryParams
import ai.senscience.nexus.delta.plugins.graph.analytics.model.GraphAnalyticsRejection
import ai.senscience.nexus.delta.plugins.graph.analytics.permissions.query
import ai.senscience.nexus.delta.plugins.graph.analytics.{GraphAnalytics, GraphAnalyticsViewsQuery}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.instances.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources.read as Read
import ai.senscience.nexus.delta.sourcing.ProgressStatistics
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.effect.IO
import io.circe.JsonObject

/**
  * The graph analytics routes.
  *
  * @param identities
  *   the identity module
  * @param aclCheck
  *   to check acls
  * @param graphAnalytics
  *   analytics the graph analytics module
  * @param fetchStatistics
  *   how to fetch the statistics for the graph analytics for a given project
  */
class GraphAnalyticsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    graphAnalytics: GraphAnalytics,
    fetchStatistics: ProjectRef => IO[ProgressStatistics],
    viewsQuery: GraphAnalyticsViewsQuery
)(implicit baseUri: BaseUri, cr: RemoteContextResolution, ordering: JsonKeyOrdering)
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  private val graphAnalyticsExceptionHandler = ExceptionHandler { case err: GraphAnalyticsRejection =>
    discardEntityAndForceEmit(err)
  }.withFallback(ElasticSearchExceptionHandler.client)

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      handleExceptions(graphAnalyticsExceptionHandler) {
        pathPrefix("graph-analytics") {
          extractCaller { implicit caller =>
            projectRef { project =>
              concat(
                get {
                  concat(
                    // Fetch relationships
                    (pathPrefix("relationships") & pathEndOrSingleSlash) {
                      authorizeFor(project, Read).apply {
                        emit(graphAnalytics.relationships(project))
                      }
                    },
                    // Fetch properties for a type
                    (pathPrefix("properties") & idSegment & pathEndOrSingleSlash) { tpe =>
                      authorizeFor(project, Read).apply {
                        emit(graphAnalytics.properties(project, tpe))
                      }
                    },
                    // Fetch the statistics
                    (pathPrefix("statistics") & pathEndOrSingleSlash) {
                      authorizeFor(project, Read).apply {
                        emit(fetchStatistics(project))
                      }
                    }
                  )
                },
                post {
                  // Search a graph analytics view
                  (pathPrefix("_search") & pathEndOrSingleSlash) {
                    authorizeFor(project, query).apply {
                      (extractQueryParams & entity(as[JsonObject])) { (qp, query) =>
                        emit(viewsQuery.query(project, query, qp))
                      }
                    }
                  }
                }
              )
            }
          }
        }
      }
    }
}
