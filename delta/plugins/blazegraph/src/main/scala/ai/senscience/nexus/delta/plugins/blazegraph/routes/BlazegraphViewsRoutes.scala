package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.model.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.permissions.{query as Query, read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks
import ai.senscience.nexus.delta.plugins.blazegraph.{BlazegraphViews, BlazegraphViewsQuery}
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives}
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.*
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.*
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes.Created
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.http.scaladsl.server.{Directive0, Route}
import org.typelevel.otel4s.trace.Tracer

/**
  * The Blazegraph views routes
  */
class BlazegraphViewsRoutes(
    views: BlazegraphViews,
    viewsQuery: BlazegraphViewsQuery,
    incomingOutgoingLinks: IncomingOutgoingLinks,
    identities: Identities,
    aclCheck: AclCheck
)(using BaseUri, RemoteContextResolution, JsonKeyOrdering, PaginationConfig, FusionConfig, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with DeltaDirectives
    with RdfMarshalling
    with BlazegraphViewsDirectives {

  private def emitMetadataOrReject(statusCode: StatusCode, io: IO[ViewResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadataOrReject(io: IO[ViewResource]): Route = emitMetadataOrReject(StatusCodes.OK, io)

  private def emitSource(io: IO[ViewResource]): Route =
    emit(io.map { resource => OriginalSource(resource, resource.value.source) })

  def routes: Route = {
    handleExceptions(BlazegraphExceptionHandler.apply) {
      concat(
        pathPrefix("views") {
          extractCaller { case given Caller =>
            projectRef { project =>
              val authorizeRead  = authorizeFor(project, Read)
              val authorizeWrite = authorizeFor(project, Write)
              // Create a view without id segment
              concat(
                routeSpan("views/<str:org>/<str:project>") {
                  (pathEndOrSingleSlash & post & authorizeWrite & jsonEntity & noRev) { source =>
                    emitMetadataOrReject(Created, views.create(project, source))
                  }
                },
                idSegment { id =>
                  concat(
                    pathEndOrSingleSlash {
                      routeSpan("views/<str:org>/<str:project>/<str:id>") {
                        concat(
                          (put & authorizeWrite & revParamOpt & pathEndOrSingleSlash & jsonEntity) {
                            case (None, source)      =>
                              // Create a view with id segment
                              emitMetadataOrReject(
                                Created,
                                views.create(id, project, source)
                              )
                            case (Some(rev), source) =>
                              // Update a view
                              emitMetadataOrReject(
                                views.update(id, project, rev, source)
                              )
                          },
                          (delete & authorizeWrite & revParam) { rev =>
                            // Deprecate a view
                            emitMetadataOrReject(views.deprecate(id, project, rev))
                          },
                          // Fetch a view
                          (get & idSegmentRef(id)) { id =>
                            val fetchRoute = authorizeRead { emit(views.fetch(id, project)) }
                            emitOrFusionRedirect(project, id, fetchRoute)
                          }
                        )
                      }
                    },
                    // Undeprecate a blazegraph view
                    (pathPrefix("undeprecate") & put & authorizeWrite & revParam & pathEndOrSingleSlash) { rev =>
                      routeSpan("views/<str:org>/<str:project>/<str:id>/undeprecate") {
                        emitMetadataOrReject(views.undeprecate(id, project, rev))
                      }
                    },
                    // Query a blazegraph view
                    (pathPrefix("sparql") & pathEndOrSingleSlash & routeSpan(
                      "views/<str:org>/<str:project>/<str:id>/sparql"
                    )) {
                      concat(
                        // Query
                        sparqlQueryResponseType { case (query, responseType) =>
                          emit(viewsQuery.query(id, project, query, responseType))
                        }
                      )
                    },
                    // Fetch a view original source
                    (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(id)) { id =>
                      (routeSpan("views/<str:org>/<str:project>/<str:id>/source") & authorizeRead) {
                        emitSource(views.fetch(id, project))
                      }
                    },
                    // Incoming/outgoing links for views
                    incomingOutgoing(id, project)
                  )
                }
              )
            }
          }
        },
        // Handle all other incoming and outgoing links
        pathPrefix(Segment) { segment =>
          extractCaller { case given Caller =>
            projectRef { project =>
              // if we are on the path /resources/{org}/{proj}/ we need to consume the {schema} segment before consuming the {id}
              consumeIdSegmentIf(segment == "resources") {
                idSegment { id => incomingOutgoing(id, project) }
              }
            }
          }
        }
      )
    }
  }

  private def consumeIdSegmentIf(condition: Boolean): Directive0 =
    if condition then idSegment.flatMap(_ => pass)
    else pass

  private def incomingOutgoing(id: IdSegment, project: ProjectRef)(using Caller) = {
    val authorizeQuery  = authorizeFor(project, Query)
    val metadataContext = ContextValue(Vocabulary.contexts.metadata)
    concat(
      (pathPrefix("incoming") & fromPaginated & pathEndOrSingleSlash & get & extractHttp4sUri) { (pagination, uri) =>
        given JsonLdEncoder[SearchResults[SparqlLink]] =
          searchResultsJsonLdEncoder(metadataContext, pagination, uri)
        (routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>/incoming") & authorizeQuery) {
          emit(incomingOutgoingLinks.incoming(id, project, pagination))
        }
      },
      (pathPrefix("outgoing") & fromPaginated & pathEndOrSingleSlash & get & extractHttp4sUri & parameter(
        "includeExternalLinks".as[Boolean] ? true
      )) { (pagination, uri, includeExternal) =>
        given JsonLdEncoder[SearchResults[SparqlLink]] =
          searchResultsJsonLdEncoder(metadataContext, pagination, uri)
        (routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>/incoming") & authorizeQuery) {
          emit(incomingOutgoingLinks.outgoing(id, project, pagination, includeExternal))
        }
      }
    )
  }
}

object BlazegraphViewsRoutes {

  def apply(
      views: BlazegraphViews,
      viewsQuery: BlazegraphViewsQuery,
      incomingOutgoingLinks: IncomingOutgoingLinks,
      identities: Identities,
      aclCheck: AclCheck
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering, PaginationConfig, FusionConfig, Tracer[IO]): Route = {
    new BlazegraphViewsRoutes(
      views,
      viewsQuery,
      incomingOutgoingLinks,
      identities,
      aclCheck
    ).routes
  }
}
