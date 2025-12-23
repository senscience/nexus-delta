package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.indexing.{configuredIndexingId, configuredIndexingProjection}
import ai.senscience.nexus.delta.elasticsearch.model.permissions
import ai.senscience.nexus.delta.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.elasticsearch.query.ConfiguredIndexQuery
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchViewsDirectives.extractQueryParams
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.routeSpan
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import io.circe.JsonObject
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

final class ConfiguredIndexRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    configuredQuery: ConfiguredIndexQuery,
    projectionDirectives: ProjectionsDirectives
)(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      concat(route)
    }

  private def indexTarget: Directive1[ConfiguredIndexQuery.Target] = pathPrefix(Segment).flatMap {
    case "_"   => provide(ConfiguredIndexQuery.Target.All)
    case value => label(value).flatMap(l => provide(ConfiguredIndexQuery.Target.Single(l)))
  }

  private def route =
    pathPrefix("index") {
      pathPrefix("configured") {
        extractCaller { case given Caller =>
          projectRef { project =>
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            val authorizeQuery = authorizeFor(project, permissions.query)
            val projection     = configuredIndexingProjection(project)
            concat(
              pathPrefix("_") {
                concat(
                  // Fetch statistics for the configured indexing on this current project
                  routeSpan("index/configured/<str:org>/<str:project>/_/statistics") {
                    (pathPrefix("statistics") & get & pathEndOrSingleSlash & authorizeRead) {
                      projectionDirectives.statistics(project, SelectFilter.latest, projection)
                    }
                  },
                  // Fetch configured indexing failures
                  routeSpan("views/<str:org>/<str:project>/documents/failures") {
                    (pathPrefix("failures") & get & authorizeWrite) {
                      projectionDirectives.indexingErrors(project, configuredIndexingId)
                    }
                  },
                  // Getting indexing status for a resource in configured indexing
                  routeSpan("index/configured/<str:org>/<str:project>/_/status") {
                    (pathPrefix("status") & authorizeRead) {
                      projectionDirectives.indexingStatus(
                        project,
                        SelectFilter.latest,
                        projection,
                        configuredQuery.refresh
                      )
                    }
                  },
                  // Manage a main indexing offset
                  routeSpan("index/configured/<str:org>/<str:project>/_/offset") {
                    (pathPrefix("offset") & pathEndOrSingleSlash) {
                      concat(
                        // Fetch a configured view offset
                        (get & authorizeRead) {
                          projectionDirectives.offset(projection)
                        },
                        // Remove an configured index offset (restart it)
                        (delete & authorizeWrite & offset("from")) { fromOffset =>
                          projectionDirectives.scheduleRestart(projection, fromOffset)
                        }
                      )
                    }
                  }
                )
              },
              routeSpan("index/configured/<str:org>/<str:project>/<str:target>/_search") {
                (indexTarget & pathPrefix("_search") & post & pathEndOrSingleSlash) { target =>
                  authorizeQuery {
                    (extractQueryParams & entity(as[JsonObject])) { (qp, query) =>
                      emit(configuredQuery.search(project, target, query, qp))
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
