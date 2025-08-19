package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViewsQuery
import ai.senscience.nexus.delta.elasticsearch.indexing.FetchIndexingView
import ai.senscience.nexus.delta.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{timeRange, *}
import ai.senscience.nexus.delta.sdk.error.ServiceError.ResourceNotFound
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.indexing.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.Projections
import akka.http.scaladsl.server.*
import cats.effect.unsafe.implicits.*

/**
  * The elasticsearch views routes
  *
  * @param identities
  *   the identity module
  * @param aclCheck
  *   to check acls
  * @param fetch
  *   how to fetch an Elasticsearch view
  * @param projections
  *   the projections module
  * @param projectionErrorsSearch
  *   the projection errors search module
  */
final class ElasticSearchIndexingRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    fetch: FetchIndexingView,
    projections: Projections,
    projectionErrorsSearch: ProjectionErrorsSearch,
    viewsQuery: ElasticSearchViewsQuery
)(implicit
    baseUri: BaseUri,
    paginationConfig: PaginationConfig,
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
              (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
                authorizeRead {
                  emit(projections.statistics(project, view.selectFilter, view.projection))
                }
              },
              // Fetch elastic search view indexing failures
              (pathPrefix("failures") & get) {
                authorizeWrite {
                  (fromPaginated & timeRange("instant") & extractHttp4sUri & pathEndOrSingleSlash) {
                    (pagination, timeRange, uri) =>
                      implicit val searchJsonLdEncoder: JsonLdEncoder[FailedElemSearchResults] =
                        failedElemSearchJsonLdEncoder(pagination, uri)
                      emit(projectionErrorsSearch(view.ref, pagination, timeRange))
                  }
                }
              },
              // Manage an elasticsearch view offset
              (pathPrefix("offset") & pathEndOrSingleSlash) {
                concat(
                  // Fetch an elasticsearch view offset
                  (get & authorizeRead) {
                    emit(projections.offset(view.projection))
                  },
                  // Remove an elasticsearch view offset (restart the view)
                  (delete & authorizeWrite) {
                    emit(projections.scheduleRestart(view.projection).as(Offset.start))
                  }
                )
              },
              // Getting indexing status for a resource in the given view
              (pathPrefix("status") & authorizeRead & iriSegment & pathEndOrSingleSlash) { resourceId =>
                emitJson(
                  projections
                    .indexingStatus(project, view.selectFilter, view.projection, resourceId)(
                      ResourceNotFound(resourceId, project)
                    )
                )
              },
              // Get elasticsearch view mapping
              (pathPrefix("_mapping") & get & authorizeWrite & pathEndOrSingleSlash) {
                emit(viewsQuery.mapping(view))
              }
            )
          }
        }
      }
    }
}

object ElasticSearchIndexingRoutes {

  /**
    * @return
    *   the [[Route]] for elasticsearch views
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      fetch: FetchIndexingView,
      projections: Projections,
      projectionErrorsSearch: ProjectionErrorsSearch,
      viewsQuery: ElasticSearchViewsQuery
  )(implicit
      baseUri: BaseUri,
      paginationConfig: PaginationConfig,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Route =
    new ElasticSearchIndexingRoutes(
      identities,
      aclCheck,
      fetch,
      projections,
      projectionErrorsSearch,
      viewsQuery
    ).routes
}
