package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchViewsDirectives
import ai.senscience.nexus.delta.plugins.blazegraph.routes.BlazegraphViewsDirectives
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.compositeviews.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.compositeviews.model.{ProjectionOffset, ProjectionStatistics}
import ai.senscience.nexus.delta.plugins.compositeviews.projections.{CompositeIndexingDetails, CompositeProjections}
import ai.senscience.nexus.delta.plugins.compositeviews.{ExpandId, FetchView}
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.offset.Offset
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import cats.syntax.all.*

class CompositeViewsIndexingRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    fetchView: FetchView,
    expandId: ExpandId,
    details: CompositeIndexingDetails,
    projections: CompositeProjections,
    projectionDirectives: ProjectionsDirectives
)(implicit
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with DeltaDirectives
    with CirceUnmarshalling
    with RdfMarshalling
    with ElasticSearchViewsDirectives
    with BlazegraphViewsDirectives {

  implicit private val offsetsSearchJsonLdEncoder: JsonLdEncoder[SearchResults[ProjectionOffset]] =
    searchResultsJsonLdEncoder(ContextValue(contexts.offset))

  implicit private val statisticsSearchJsonLdEncoder: JsonLdEncoder[SearchResults[ProjectionStatistics]] =
    searchResultsJsonLdEncoder(ContextValue(contexts.statistics))

  private def fetchActiveView =
    (projectRef & idSegment).tflatMap { case (project, idSegment) =>
      onSuccess(fetchView(idSegment, project).unsafeToFuture())
    }

  def routes: Route =
    handleExceptions(CompositeViewExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { implicit caller =>
          fetchActiveView { view =>
            val project        = view.project
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              // Manage composite view offsets
              (pathPrefix("offset") & pathEndOrSingleSlash) {
                concat(
                  // Fetch all composite view offsets
                  (get & authorizeRead) {
                    emit(fetchOffsets(view))
                  },
                  // Remove all composite view offsets (restart the view)
                  (delete & authorizeWrite) {
                    emit(fullRestart(view))
                  }
                )
              },
              // Fetch composite indexing description
              (pathPrefix("description") & pathEndOrSingleSlash & get) {
                authorizeRead {
                  emit(details.description(view))
                }
              },
              // Fetch composite view statistics
              (pathPrefix("statistics") & pathEndOrSingleSlash & get) {
                authorizeRead {
                  emit(details.statistics(view))
                }
              },
              // Fetch elastic search view indexing failures
              (pathPrefix("failures") & get) {
                authorizeWrite {
                  projectionDirectives.indexingErrors(view.ref)
                }
              },
              pathPrefix("projections") {
                concat(
                  // Manage all views' projections offsets
                  (pathPrefix("_") & pathPrefix("offset") & pathEndOrSingleSlash) {
                    concat(
                      // Fetch all composite view projection offsets
                      (get & authorizeRead) {
                        emit(details.offsets(view.indexingRef))
                      },
                      // Remove all composite view projection offsets
                      (delete & authorizeFor(project, Write)) {
                        emit(fullRebuild(view))
                      }
                    )
                  },
                  // Fetch all views' projections statistics
                  (get & pathPrefix("_") & pathPrefix("statistics") & pathEndOrSingleSlash) {
                    authorizeRead {
                      emit(details.statistics(view))
                    }
                  },
                  // Manage a views' projection offset
                  (idSegment & pathPrefix("offset") & pathEndOrSingleSlash) { projectionId =>
                    concat(
                      // Fetch a composite view projection offset
                      (get & authorizeRead) {
                        emit(projectionOffsets(view, projectionId))
                      },
                      // Remove a composite view projection offset
                      (delete & authorizeWrite) {
                        emit(partialRebuild(view, projectionId))
                      }
                    )
                  },
                  // Fetch a views' projection statistics
                  (get & idSegment & pathPrefix("statistics") & pathEndOrSingleSlash) { projectionId =>
                    authorizeRead {
                      emit(projectionStatistics(view, projectionId))
                    }
                  }
                )
              },
              pathPrefix("sources") {
                concat(
                  // Fetch all views' sources statistics
                  (get & pathPrefix("_") & pathPrefix("statistics") & pathEndOrSingleSlash) {
                    authorizeFor(project, Read).apply {
                      emit(details.statistics(view))
                    }
                  },
                  // Fetch a views' sources statistics
                  (get & idSegment & pathPrefix("statistics") & pathEndOrSingleSlash) { sourceId =>
                    authorizeFor(project, Read).apply {
                      emit(sourceStatistics(view, sourceId))
                    }
                  }
                )
              }
            )
          }
        }
      }
    }

  private def fetchOffsets(view: ActiveViewDef) =
    details.offsets(view.indexingRef)

  private def projectionOffsets(view: ActiveViewDef, projectionId: IdSegment) =
    for {
      projection <- fetchProjection(view, projectionId)
      offsets    <- details.projectionOffsets(view.indexingRef, projection.id)
    } yield offsets

  private def projectionStatistics(view: ActiveViewDef, projectionId: IdSegment) =
    for {
      projection <- fetchProjection(view, projectionId)
      offsets    <- details.projectionStatistics(view, projection.id)
    } yield offsets

  private def sourceStatistics(view: ActiveViewDef, sourceId: IdSegment) =
    for {
      source  <- fetchSource(view, sourceId)
      offsets <- details.sourceStatistics(view, source.id)
    } yield offsets

  private def fullRestart(view: ActiveViewDef)(implicit s: Subject) =
    for {
      offsets <- details.offsets(view.indexingRef)
      _       <- projections.scheduleFullRestart(view.ref)
    } yield offsets.map(_.copy(offset = Offset.Start))

  private def fullRebuild(view: ActiveViewDef)(implicit s: Subject) =
    for {
      offsets <- details.offsets(view.indexingRef)
      _       <- projections.scheduleFullRebuild(view.ref)
    } yield offsets.map(_.copy(offset = Offset.Start))

  private def partialRebuild(view: ActiveViewDef, projectionId: IdSegment)(implicit s: Subject) =
    for {
      projection <- fetchProjection(view, projectionId)
      offsets    <- details.projectionOffsets(view.indexingRef, projection.id)
      _          <- projections.schedulePartialRebuild(view.ref, projection.id)
    } yield offsets.map(_.copy(offset = Offset.Start))

  private def fetchProjection(view: ActiveViewDef, projectionId: IdSegment) =
    expandId(projectionId, view.project).flatMap { id =>
      IO.fromEither(view.projection(id))
    }

  private def fetchSource(view: ActiveViewDef, sourceId: IdSegment) =
    expandId(sourceId, view.project).flatMap { id =>
      IO.fromEither(view.source(id))
    }

}

object CompositeViewsIndexingRoutes {

  /**
    * @return
    *   the [[Route]] for composite views.
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      fetchView: FetchView,
      expandId: ExpandId,
      statistics: CompositeIndexingDetails,
      projections: CompositeProjections,
      projectionDirectives: ProjectionsDirectives
  )(implicit
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Route =
    new CompositeViewsIndexingRoutes(
      identities,
      aclCheck,
      fetchView,
      expandId,
      statistics,
      projections,
      projectionDirectives
    ).routes
}
