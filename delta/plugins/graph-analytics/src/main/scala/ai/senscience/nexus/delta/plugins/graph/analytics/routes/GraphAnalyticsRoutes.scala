package ai.senscience.nexus.delta.plugins.graph.analytics.routes

import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchExceptionHandler
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchViewsDirectives.extractQueryParams
import ai.senscience.nexus.delta.plugins.graph.analytics.model.GraphAnalyticsRejection
import ai.senscience.nexus.delta.plugins.graph.analytics.permissions.query
import ai.senscience.nexus.delta.plugins.graph.analytics.{GraphAnalytics, GraphAnalyticsViewsQuery}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources.read as Read
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, Route}
import org.typelevel.otel4s.trace.Tracer

/**
  * The graph analytics routes.
  */
class GraphAnalyticsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    graphAnalytics: GraphAnalytics,
    projectionsDirectives: ProjectionsDirectives,
    viewsQuery: GraphAnalyticsViewsQuery
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
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
          extractCaller { case given Caller =>
            projectRef { project =>
              val authorizeRead  = authorizeFor(project, Read)
              val authorizeQuery = authorizeFor(project, query)
              concat(
                get {
                  concat(
                    // Fetch relationships
                    (pathPrefix("relationships") & authorizeRead & pathEndOrSingleSlash) {
                      emit(graphAnalytics.relationships(project))
                    },
                    // Fetch properties for a type
                    (pathPrefix("properties") & idSegment & authorizeRead & pathEndOrSingleSlash) { tpe =>
                      emit(graphAnalytics.properties(project, tpe))
                    },
                    // Fetch the statistics
                    (pathPrefix("statistics") & authorizeRead & pathEndOrSingleSlash) {
                      projectionsDirectives.statistics(
                        project,
                        SelectFilter.latest,
                        GraphAnalytics.projectionName(project)
                      )
                    }
                  )
                },
                // Search a graph analytics view
                (post & pathPrefix("_search") & authorizeQuery & pathEndOrSingleSlash) {
                  (extractQueryParams & jsonObjectEntity) { (qp, query) =>
                    emit(viewsQuery.query(project, query, qp))
                  }
                }
              )
            }
          }
        }
      }
    }
}
