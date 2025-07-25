package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViewsQuery
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchIndexingRoutes.FetchIndexingView
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{timeRange, *}
import ai.senscience.nexus.delta.sdk.error.ServiceError.ResourceNotFound
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sourcing.ProgressStatistics
import ai.senscience.nexus.delta.sourcing.model.FailedElemLogRow.FailedElemData
import ai.senscience.nexus.delta.sourcing.model.{FailedElemLogRow, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
import akka.http.scaladsl.server.*
import cats.effect.IO
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.*

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
  * @param projectionErrors
  *   the projection errors module
  */
final class ElasticSearchIndexingRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    fetch: FetchIndexingView,
    projections: Projections,
    projectionErrors: ProjectionErrors,
    viewsQuery: ElasticSearchViewsQuery
)(implicit
    baseUri: BaseUri,
    paginationConfig: PaginationConfig,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  implicit private val viewStatisticEncoder: Encoder.AsObject[ProgressStatistics] =
    deriveEncoder[ProgressStatistics].mapJsonObject(_.add(keywords.tpe, "ViewStatistics".asJson))

  implicit private val viewStatisticJsonLdEncoder: JsonLdEncoder[ProgressStatistics] =
    JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.statistics))

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { implicit caller =>
          projectRef { project =>
            concat(
              idSegment { id =>
                concat(
                  // Fetch an elasticsearch view statistics
                  (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
                    authorizeFor(project, Read).apply {
                      emit(
                        fetch(id, project)
                          .flatMap(v => projections.statistics(project, v.selectFilter, v.projection))
                      )
                    }
                  },
                  // Fetch elastic search view indexing failures
                  (pathPrefix("failures") & get) {
                    authorizeFor(project, Write).apply {
                      (fromPaginated & timeRange("instant") & extractHttp4sUri & pathEndOrSingleSlash) {
                        (pagination, timeRange, uri) =>
                          implicit val searchJsonLdEncoder: JsonLdEncoder[SearchResults[FailedElemData]] =
                            searchResultsJsonLdEncoder(FailedElemLogRow.context, pagination, uri)
                          emit(
                            fetch(id, project)
                              .flatMap { view =>
                                projectionErrors.search(view.ref, pagination, timeRange)
                              }
                          )
                      }
                    }
                  },
                  // Manage an elasticsearch view offset
                  (pathPrefix("offset") & pathEndOrSingleSlash) {
                    concat(
                      // Fetch an elasticsearch view offset
                      (get & authorizeFor(project, Read)) {
                        emit(
                          fetch(id, project)
                            .flatMap(v => projections.offset(v.projection))
                        )
                      },
                      // Remove an elasticsearch view offset (restart the view)
                      (delete & authorizeFor(project, Write)) {
                        emit(
                          fetch(id, project)
                            .flatMap { v => projections.scheduleRestart(v.projection) }
                            .as(Offset.start)
                        )
                      }
                    )
                  },
                  // Getting indexing status for a resource in the given view
                  (pathPrefix("status") & authorizeFor(project, Read) & iriSegment & pathEndOrSingleSlash) {
                    resourceId =>
                      emit(
                        fetch(id, project)
                          .flatMap { view =>
                            projections
                              .indexingStatus(project, view.selectFilter, view.projection, resourceId)(
                                ResourceNotFound(resourceId, project)
                              )
                              .map(_.asJson)
                          }
                      )
                  },
                  // Get elasticsearch view mapping
                  (pathPrefix("_mapping") & get & pathEndOrSingleSlash) {
                    emit(viewsQuery.mapping(id, project))
                  }
                )
              }
            )
          }
        }
      }
    }
}

object ElasticSearchIndexingRoutes {

  type FetchIndexingView = (IdSegment, ProjectRef) => IO[ActiveViewDef]

  /**
    * @return
    *   the [[Route]] for elasticsearch views
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      fetch: FetchIndexingView,
      projections: Projections,
      projectionErrors: ProjectionErrors,
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
      projectionErrors,
      viewsQuery
    ).routes
}
