package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.FetchIndexingView
import ai.senscience.nexus.delta.plugins.blazegraph.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.indexing.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.Projections
import akka.http.scaladsl.server.*
import cats.effect.unsafe.implicits.*

class BlazegraphViewsIndexingRoutes(
    fetch: FetchIndexingView,
    identities: Identities,
    aclCheck: AclCheck,
    projections: Projections,
    projectionDirectives: ProjectionsDirectives
)(implicit
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with DeltaDirectives
    with RdfMarshalling
    with BlazegraphViewsDirectives {

  private def fetchActiveView =
    (projectRef & idSegment).tflatMap { case (project, idSegment) =>
      onSuccess(fetch(idSegment, project).unsafeToFuture())
    }

  def routes: Route =
    handleExceptions(BlazegraphExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { implicit caller =>
          fetchActiveView { view =>
            val project        = view.ref.project
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              // Fetch a blazegraph view statistics
              (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
                authorizeRead {
                  emit(projections.statistics(project, view.selectFilter, view.projection))
                }
              },
              // Fetch blazegraph view indexing failures
              (pathPrefix("failures") & get) {
                authorizeWrite {
                  projectionDirectives.indexingErrors(view.ref)
                }
              },
              // Manage a blazegraph view offset
              (pathPrefix("offset") & pathEndOrSingleSlash) {
                concat(
                  // Fetch a blazegraph view offset
                  (get & authorizeRead) {
                    emit(projections.offset(view.projection))
                  },
                  // Remove a blazegraph view offset (restart the view)
                  (delete & authorizeWrite) {
                    emit(projections.scheduleRestart(view.projection).as(Offset.start))
                  }
                )
              },
              // Getting indexing status for a resource in the given view
              (pathPrefix("status") & authorizeRead) {
                projectionDirectives.indexingStatus(project, view.selectFilter, view.projection)
              }
            )
          }
        }
      }
    }
}

object BlazegraphViewsIndexingRoutes {

  /**
    * @return
    *   the [[Route]] for BlazegraphViews
    */
  def apply(
      fetch: FetchIndexingView,
      identities: Identities,
      aclCheck: AclCheck,
      projections: Projections,
      projectionDirectives: ProjectionsDirectives
  )(implicit
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Route = {
    new BlazegraphViewsIndexingRoutes(
      fetch,
      identities,
      aclCheck,
      projections,
      projectionDirectives
    ).routes
  }
}
