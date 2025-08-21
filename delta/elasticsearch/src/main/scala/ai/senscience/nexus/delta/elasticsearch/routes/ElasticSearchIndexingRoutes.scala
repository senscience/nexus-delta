package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.elasticsearch.indexing.FetchIndexingView
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchIndexingRoutes.FetchMapping
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import akka.http.scaladsl.server.*
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.Json

/**
  * The elasticsearch views indexing routes
  */
final class ElasticSearchIndexingRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    fetch: FetchIndexingView,
    projectionDirectives: ProjectionsDirectives,
    fetchMapping: FetchMapping
)(implicit
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  private def fetchActiveView =
    (projectRef & idSegment).tflatMap { case (project, idSegment) =>
      onSuccess(fetch(idSegment, project).unsafeToFuture())
    }

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { implicit caller =>
          fetchActiveView { view =>
            val project        = view.ref.project
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              // Fetch an elasticsearch view statistics
              (pathPrefix("statistics") & get & pathEndOrSingleSlash & authorizeRead) {
                projectionDirectives.statistics(project, view.selectFilter, view.projection)
              },
              // Fetch elastic search view indexing failures
              (pathPrefix("failures") & get & authorizeWrite) {
                projectionDirectives.indexingErrors(view.ref)
              },
              // Manage an elasticsearch view offset
              (pathPrefix("offset") & pathEndOrSingleSlash) {
                concat(
                  // Fetch an elasticsearch view offset
                  (get & authorizeRead) {
                    projectionDirectives.offset(view.projection)
                  },
                  // Remove an elasticsearch view offset (restart the view)
                  (delete & authorizeWrite) {
                    projectionDirectives.scheduleRestart(view.projection)
                  }
                )
              },
              // Getting indexing status for a resource in the given view
              (pathPrefix("status") & authorizeRead) {
                projectionDirectives.indexingStatus(project, view.selectFilter, view.projection)
              },
              // Get elasticsearch view mapping
              (pathPrefix("_mapping") & get & authorizeWrite & pathEndOrSingleSlash) {
                emit(fetchMapping(view))
              }
            )
          }
        }
      }
    }
}

object ElasticSearchIndexingRoutes {

  type FetchMapping = ActiveViewDef => IO[Json]

  /**
    * @return
    *   the [[Route]] for elasticsearch views indexing
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      fetch: FetchIndexingView,
      projectionDirectives: ProjectionsDirectives,
      client: ElasticSearchClient
  )(implicit
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): ElasticSearchIndexingRoutes =
    new ElasticSearchIndexingRoutes(
      identities,
      aclCheck,
      fetch,
      projectionDirectives,
      (view: ActiveViewDef) => client.mapping(view.index)
    )
}
