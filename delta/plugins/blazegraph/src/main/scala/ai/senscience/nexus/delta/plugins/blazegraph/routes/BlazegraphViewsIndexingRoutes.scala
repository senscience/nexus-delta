package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.model.permissions
import ai.senscience.nexus.delta.plugins.blazegraph.model.permissions.write as Write
import ai.senscience.nexus.delta.plugins.blazegraph.routes.BlazegraphViewsIndexingRoutes.FetchIndexingView
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment}
import ai.senscience.nexus.delta.sourcing.ProgressStatistics
import ai.senscience.nexus.delta.sourcing.model.FailedElemLogRow.FailedElemData
import ai.senscience.nexus.delta.sourcing.model.{FailedElemLogRow, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
import akka.http.scaladsl.server.Route
import cats.effect.IO
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.*

class BlazegraphViewsIndexingRoutes(
    fetch: FetchIndexingView,
    identities: Identities,
    aclCheck: AclCheck,
    projections: Projections,
    projectionErrors: ProjectionErrors
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering,
    pc: PaginationConfig
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with DeltaDirectives
    with RdfMarshalling
    with BlazegraphViewsDirectives {

  implicit private val viewStatisticEncoder: Encoder.AsObject[ProgressStatistics] =
    deriveEncoder[ProgressStatistics].mapJsonObject(_.add(keywords.tpe, "ViewStatistics".asJson))

  implicit private val viewStatisticJsonLdEncoder: JsonLdEncoder[ProgressStatistics] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.statistics))

  def routes: Route =
    handleExceptions(BlazegraphExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { implicit caller =>
          projectRef { implicit project =>
            idSegment { id =>
              concat(
                // Fetch a blazegraph view statistics
                (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
                  authorizeFor(project, permissions.read).apply {
                    emit(
                      fetch(id, project)
                        .flatMap(v => projections.statistics(project, v.selectFilter, v.projection))
                    )
                  }
                },
                // Fetch blazegraph view indexing failures
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
                // Manage an blazegraph view offset
                (pathPrefix("offset") & pathEndOrSingleSlash) {
                  concat(
                    // Fetch a blazegraph view offset
                    (get & authorizeFor(project, permissions.read)) {
                      emit(
                        fetch(id, project)
                          .flatMap(v => projections.offset(v.projection))
                      )
                    },
                    // Remove an blazegraph view offset (restart the view)
                    (delete & authorizeFor(project, Write)) {
                      emit(
                        fetch(id, project)
                          .flatMap { r => projections.scheduleRestart(r.projection) }
                          .as(Offset.start)
                      )
                    }
                  )
                }
              )
            }
          }
        }
      }
    }
}

object BlazegraphViewsIndexingRoutes {

  type FetchIndexingView = (IdSegment, ProjectRef) => IO[ActiveViewDef]

  /**
    * @return
    *   the [[Route]] for BlazegraphViews
    */
  def apply(
      fetch: FetchIndexingView,
      identities: Identities,
      aclCheck: AclCheck,
      projections: Projections,
      projectionErrors: ProjectionErrors
  )(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering,
      pc: PaginationConfig
  ): Route = {
    new BlazegraphViewsIndexingRoutes(
      fetch,
      identities,
      aclCheck,
      projections,
      projectionErrors
    ).routes
  }
}
