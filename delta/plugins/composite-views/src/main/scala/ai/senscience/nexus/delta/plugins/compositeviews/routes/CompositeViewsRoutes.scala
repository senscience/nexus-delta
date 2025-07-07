package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.plugins.blazegraph.routes.BlazegraphViewsDirectives
import ai.senscience.nexus.delta.plugins.compositeviews.model.ViewResource
import ai.senscience.nexus.delta.plugins.compositeviews.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.compositeviews.{BlazegraphQuery, CompositeViews, ElasticSearchQuery}
import ai.senscience.nexus.delta.plugins.elasticsearch.routes.ElasticSearchViewsDirectives
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.server.Route
import cats.effect.IO
import io.circe.{Json, JsonObject}

/**
  * Composite views routes.
  */
class CompositeViewsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    views: CompositeViews,
    blazegraphQuery: BlazegraphQuery,
    elasticSearchQuery: ElasticSearchQuery
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering,
    fusionConfig: FusionConfig
) extends AuthDirectives(identities, aclCheck)
    with DeltaDirectives
    with CirceUnmarshalling
    with RdfMarshalling
    with ElasticSearchViewsDirectives
    with BlazegraphViewsDirectives {

  private def emitMetadata(statusCode: StatusCode, io: IO[ViewResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[ViewResource]): Route = emitMetadata(OK, io)

  private def emitSource(io: IO[ViewResource]) = emit(io.map(_.value.source))

  def routes: Route = {
    handleExceptions(CompositeViewExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { implicit caller =>
          projectRef { implicit project =>
            concat(
              // Create a view without id segment
              (pathEndOrSingleSlash & post & entity(as[Json]) & noParameter("rev")) { source =>
                authorizeFor(project, Write).apply {
                  emitMetadata(Created, views.create(project, source))
                }
              },
              idSegment { viewId =>
                concat(
                  pathEndOrSingleSlash {
                    concat(
                      put {
                        authorizeFor(project, Write).apply {
                          (parameter("rev".as[Int].?) & pathEndOrSingleSlash & entity(as[Json])) {
                            case (None, source)      =>
                              // Create a view with id segment
                              emitMetadata(Created, views.create(viewId, project, source))
                            case (Some(rev), source) =>
                              // Update a view
                              emitMetadata(views.update(viewId, project, rev, source))
                          }
                        }
                      },
                      // Deprecate a view
                      (delete & parameter("rev".as[Int])) { rev =>
                        authorizeFor(project, Write).apply {
                          emitMetadata(views.deprecate(viewId, project, rev))
                        }
                      },
                      // Fetch a view
                      (get & idSegmentRef(viewId)) { id =>
                        emitOrFusionRedirect(
                          project,
                          id,
                          authorizeFor(project, Read).apply {
                            emit(views.fetch(id, project))
                          }
                        )
                      }
                    )
                  },
                  // Undeprecate a view
                  (pathPrefix("undeprecate") & put & pathEndOrSingleSlash & parameter("rev".as[Int])) { rev =>
                    authorizeFor(project, Write).apply {
                      emitMetadata(views.undeprecate(viewId, project, rev))
                    }
                  },
                  // Fetch a view original source
                  (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(viewId)) { id =>
                    authorizeFor(project, Read).apply {
                      emitSource(views.fetch(id, project))
                    }
                  },
                  pathPrefix("projections") {
                    concat(
                      // Query all composite views' sparql projections namespaces
                      (pathPrefix("_") & pathPrefix("sparql") & pathEndOrSingleSlash) {
                        ((get & parameter("query".as[SparqlQuery])) | (post & entity(as[SparqlQuery]))) { query =>
                          queryResponseType.apply { responseType =>
                            emit(blazegraphQuery.queryProjections(viewId, project, query, responseType))
                          }
                        }
                      },
                      // Query a composite views' sparql projection namespace
                      (idSegment & pathPrefix("sparql") & pathEndOrSingleSlash) { projectionId =>
                        ((get & parameter("query".as[SparqlQuery])) | (post & entity(as[SparqlQuery]))) { query =>
                          queryResponseType.apply { responseType =>
                            emit(blazegraphQuery.query(viewId, projectionId, project, query, responseType))
                          }
                        }
                      },
                      // Query all composite views' elasticsearch projections indices
                      (pathPrefix("_") & pathPrefix("_search") & pathEndOrSingleSlash & post) {
                        (extractQueryParams & entity(as[JsonObject])) { (qp, query) =>
                          emit(elasticSearchQuery.queryProjections(viewId, project, query, qp))
                        }
                      },
                      // Query a composite views' elasticsearch projection index
                      (idSegment & pathPrefix("_search") & pathEndOrSingleSlash & post) { projectionId =>
                        (extractQueryParams & entity(as[JsonObject])) { (qp, query) =>
                          emit(elasticSearchQuery.query(viewId, projectionId, project, query, qp))
                        }
                      }
                    )
                  },
                  // Query the common blazegraph namespace for the composite view
                  (pathPrefix("sparql") & pathEndOrSingleSlash) {
                    concat(
                      ((get & parameter("query".as[SparqlQuery])) | (post & entity(as[SparqlQuery]))) { query =>
                        queryResponseType.apply { responseType =>
                          emit(blazegraphQuery.query(viewId, project, query, responseType))
                        }
                      }
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
  )(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering,
      fusionConfig: FusionConfig
  ): Route =
    new CompositeViewsRoutes(
      identities,
      aclCheck,
      views,
      blazegraphQuery,
      elasticSearchQuery
    ).routes
}
