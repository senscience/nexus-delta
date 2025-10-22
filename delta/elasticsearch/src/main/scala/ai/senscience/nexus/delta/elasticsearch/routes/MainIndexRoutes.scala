package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.indexing.{mainIndexingId, mainIndexingProjection, MainRestartScheduler}
import ai.senscience.nexus.delta.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.elasticsearch.model.{defaultViewId, permissions}
import ai.senscience.nexus.delta.elasticsearch.query.MainIndexQuery
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchViewsDirectives.extractQueryParams
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.Root
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.routeSpan
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import io.circe.JsonObject
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.{Directive, Route}
import org.typelevel.otel4s.trace.Tracer

final class MainIndexRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    mainIndexQuery: MainIndexQuery,
    restartScheduler: MainRestartScheduler,
    projectionDirectives: ProjectionsDirectives
)(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  private def defaultViewSegment: Directive[Unit] =
    idSegment.flatMap {
      case IdSegment.StringSegment(string) if string == "documents" => tprovide(())
      case IdSegment.IriSegment(iri) if iri == defaultViewId        => tprovide(())
      case _                                                        => reject()
    }

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      concat(views, jobs)
    }

  private def views = pathPrefix("views") {
    extractCaller { case given Caller =>
      projectRef { project =>
        val authorizeRead  = authorizeFor(project, Read)
        val authorizeWrite = authorizeFor(project, Write)
        val authorizeQuery = authorizeFor(project, permissions.query)
        val projection     = mainIndexingProjection(project)
        defaultViewSegment {
          concat(
            // Fetch statistics for the main indexing on this current project
            routeSpan("views/<str:org>/<str:project>/documents/statistics") {
              (pathPrefix("statistics") & get & pathEndOrSingleSlash & authorizeRead) {
                projectionDirectives.statistics(project, SelectFilter.latest, projection)
              }
            },
            // Fetch main view indexing failures
            routeSpan("views/<str:org>/<str:project>/documents/failures") {
              (pathPrefix("failures") & get & authorizeWrite) {
                projectionDirectives.indexingErrors(project, mainIndexingId)
              }
            },
            // Manage a main indexing offset
            routeSpan("views/<str:org>/<str:project>/documents/offsets") {
              (pathPrefix("offset") & pathEndOrSingleSlash) {
                concat(
                  // Fetch an elasticsearch view offset
                  (get & authorizeRead) {
                    projectionDirectives.offset(projection)
                  },
                  // Remove an main indexing offset (restart the view)
                  (delete & authorizeWrite & offset("from")) { fromOffset =>
                    projectionDirectives.scheduleRestart(projection, fromOffset)
                  }
                )
              }
            },
            // Getting indexing status for a resource in the main view
            routeSpan("views/<str:org>/<str:project>/documents/status") {
              (pathPrefix("status") & authorizeRead) {
                projectionDirectives.indexingStatus(project, SelectFilter.latest, projection)
              }
            },
            // Query default indexing for this given project
            routeSpan("views/<str:org>/<str:project>/documents/_search") {
              (pathPrefix("_search") & post & pathEndOrSingleSlash) {
                authorizeQuery {
                  (extractQueryParams & entity(as[JsonObject])) { (qp, query) =>
                    emit(mainIndexQuery.search(project, query, qp))
                  }
                }
              }
            }
          )
        }
      }
    }
  }

  private def jobs =
    (pathPrefix("jobs") & pathPrefix("main") & pathPrefix("reindex")) {
      extractCaller { case given Caller =>
        val authorizeRootWrite = authorizeFor(Root, Write)
        (post & authorizeRootWrite & offset("from") & pathEndOrSingleSlash) { offset =>
          emit(
            StatusCodes.Accepted,
            restartScheduler.run(offset).start.void
          )
        }
      }
    }
}
