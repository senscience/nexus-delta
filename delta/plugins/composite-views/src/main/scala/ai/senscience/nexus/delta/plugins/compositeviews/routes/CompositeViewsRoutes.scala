package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchViewsDirectives
import ai.senscience.nexus.delta.plugins.blazegraph.routes.BlazegraphViewsDirectives
import ai.senscience.nexus.delta.plugins.compositeviews.model.ViewResource
import ai.senscience.nexus.delta.plugins.compositeviews.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.compositeviews.{BlazegraphQuery, CompositeViews, ElasticSearchQuery}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes.{Created, OK}
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

/**
  * Composite views routes.
  */
class CompositeViewsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    views: CompositeViews,
    blazegraphQuery: BlazegraphQuery,
    elasticSearchQuery: ElasticSearchQuery
)(using BaseUri, RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with DeltaDirectives
    with CirceUnmarshalling
    with ElasticSearchViewsDirectives
    with BlazegraphViewsDirectives {

  private def emitMetadata(statusCode: StatusCode, io: IO[ViewResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[ViewResource]): Route = emitMetadata(OK, io)

  private def emitSource(io: IO[ViewResource]) = emit(io.map(_.value.source))

  def routes: Route = {
    handleExceptions(CompositeViewExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { case given Caller =>
          projectRef { project =>
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              // Create a view without id segment
              (pathEndOrSingleSlash & post & authorizeWrite & jsonEntity & noRev) { source =>
                emitMetadata(Created, views.create(project, source))
              },
              idSegment { viewId =>
                def viewIdRef = idSegmentRef(viewId)
                concat(
                  pathEndOrSingleSlash {
                    concat(
                      (put & authorizeWrite & revParamOpt & jsonEntity & pathEndOrSingleSlash) {
                        case (None, source)      =>
                          // Create a view with id segment
                          emitMetadata(Created, views.create(viewId, project, source))
                        case (Some(rev), source) =>
                          // Update a view
                          emitMetadata(views.update(viewId, project, rev, source))
                      },
                      // Deprecate a view
                      (delete & revParam & authorizeWrite) { rev =>
                        emitMetadata(views.deprecate(viewId, project, rev))
                      },
                      // Fetch a view
                      (get & viewIdRef) { id =>
                        val fetchRoute = authorizeRead { emit(views.fetch(id, project)) }
                        emitOrFusionRedirect(project, id, fetchRoute)
                      }
                    )
                  },
                  // Undeprecate a view
                  (pathPrefix("undeprecate") & put & authorizeWrite & pathEndOrSingleSlash & revParam) { rev =>
                    emitMetadata(views.undeprecate(viewId, project, rev))
                  },
                  // Fetch a view original source
                  (pathPrefix("source") & get & authorizeRead & pathEndOrSingleSlash & viewIdRef) { id =>
                    emitSource(views.fetch(id, project))
                  },
                  (pathPrefix("projections") & idSegment) { projection =>
                    val projectionOpt = underscoreToOption(projection)
                    concat(
                      (pathPrefix("sparql") & sparqlQueryResponseType & pathEndOrSingleSlash) {
                        case (query, responseType) =>
                          projectionOpt match {
                            case Some(projectionId) =>
                              // Query a composite views' sparql projection namespace
                              emit(blazegraphQuery.query(viewId, projectionId, project, query, responseType))
                            case None               =>
                              // Query all composite views' sparql projections namespaces
                              emit(blazegraphQuery.queryProjections(viewId, project, query, responseType))
                          }
                      },
                      // Query all composite views' elasticsearch projections indices
                      (post & pathPrefix("_search") & pathEndOrSingleSlash & elasticSearchRequest) { request =>
                        projectionOpt match {
                          case Some(projectionId) =>
                            // Query a composite views' elasticsearch projection index
                            emit(elasticSearchQuery.query(viewId, projectionId, project, request))
                          case None               =>
                            // Query all composite views' elasticsearch projections indices
                            emit(elasticSearchQuery.queryProjections(viewId, project, request))
                        }
                      }
                    )
                  },
                  // Query the common blazegraph namespace for the composite view
                  (pathPrefix("sparql") & sparqlQueryResponseType & pathEndOrSingleSlash) {
                    case (query, responseType) =>
                      emit(blazegraphQuery.query(viewId, project, query, responseType))
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

object CompositeViewsRoutes {

  /**
    * @return
    *   the [[Route]] for composite views.
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      views: CompositeViews,
      blazegraphQuery: BlazegraphQuery,
      elasticSearchQuery: ElasticSearchQuery
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO]): Route =
    new CompositeViewsRoutes(
      identities,
      aclCheck,
      views,
      blazegraphQuery,
      elasticSearchQuery
    ).routes
}
