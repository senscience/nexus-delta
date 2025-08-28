package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.elasticsearch.indexing.{mainIndexingId, mainIndexingProjection, MainRestartScheduler}
import ai.senscience.nexus.delta.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.elasticsearch.model.{defaultViewId, permissions}
import ai.senscience.nexus.delta.elasticsearch.query.MainIndexQuery
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchViewsDirectives.extractQueryParams
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.Root
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{offset, *}
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive, Route}
import io.circe.JsonObject

final class MainIndexRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    mainIndexQuery: MainIndexQuery,
    restartScheduler: MainRestartScheduler,
    projectionDirectives: ProjectionsDirectives
)(implicit cr: RemoteContextResolution, ordering: JsonKeyOrdering)
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
    extractCaller { implicit caller =>
      projectRef { project =>
        val authorizeRead  = authorizeFor(project, Read)
        val authorizeWrite = authorizeFor(project, Write)
        val authorizeQuery = authorizeFor(project, permissions.query)
        val projection     = mainIndexingProjection(project)
        defaultViewSegment {
          concat(
            // Fetch statistics for the main indexing on this current project
            (pathPrefix("statistics") & get & pathEndOrSingleSlash & authorizeRead) {
              projectionDirectives.statistics(project, SelectFilter.latest, projection)
            },
            // Fetch main view indexing failures
            (pathPrefix("failures") & get & authorizeWrite) {
              projectionDirectives.indexingErrors(project, mainIndexingId)
            },
            // Manage a main indexing offset
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
            },
            // Getting indexing status for a resource in the main view
            (pathPrefix("status") & authorizeRead) {
              projectionDirectives.indexingStatus(project, SelectFilter.latest, projection)
            },
            // Query default indexing for this given project
            (pathPrefix("_search") & post & pathEndOrSingleSlash) {
              authorizeQuery {
                (extractQueryParams & entity(as[JsonObject])) { (qp, query) =>
                  emit(mainIndexQuery.search(project, query, qp))
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
      extractCaller { implicit caller =>
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
