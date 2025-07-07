package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.plugins.blazegraph.model.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.permissions.{query as Query, read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks
import ai.senscience.nexus.delta.plugins.blazegraph.{BlazegraphViews, BlazegraphViewsQuery}
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.{OriginalSource, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.*
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.{Directive0, Route}
import cats.effect.IO
import io.circe.Json

/**
  * The Blazegraph views routes
  */
class BlazegraphViewsRoutes(
    views: BlazegraphViews,
    viewsQuery: BlazegraphViewsQuery,
    incomingOutgoingLinks: IncomingOutgoingLinks,
    identities: Identities,
    aclCheck: AclCheck
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering,
    pc: PaginationConfig,
    fusionConfig: FusionConfig
) extends AuthDirectives(identities, aclCheck)
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
          extractCaller { implicit caller =>
            projectRef { implicit project =>
              val authorizeRead  = authorizeFor(project, Read)
              val authorizeWrite = authorizeFor(project, Write)
              // Create a view without id segment
              concat(
                (pathEndOrSingleSlash & post & entity(as[Json]) & noParameter("rev")) { source =>
                  authorizeWrite {
                    emitMetadataOrReject(Created, views.create(project, source))
                  }
                },
                idSegment { id =>
                  concat(
                    pathEndOrSingleSlash {
                      concat(
                        put {
                          authorizeWrite {
                            (parameter("rev".as[Int].?) & pathEndOrSingleSlash & entity(as[Json])) {
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
                            }
                          }
                        },
                        (delete & parameter("rev".as[Int])) { rev =>
                          // Deprecate a view
                          authorizeWrite {
                            emitMetadataOrReject(
                              views.deprecate(id, project, rev)
                            )
                          }
                        },
                        // Fetch a view
                        (get & idSegmentRef(id)) { id =>
                          emitOrFusionRedirect(
                            project,
                            id,
                            authorizeRead {
                              emit(views.fetch(id, project))
                            }
                          )
                        }
                      )
                    },
                    // Undeprecate a blazegraph view
                    (pathPrefix("undeprecate") & put & parameter("rev".as[Int]) &
                      authorizeWrite & pathEndOrSingleSlash) { rev =>
                      emitMetadataOrReject(
                        views.undeprecate(id, project, rev)
                      )
                    },
                    // Query a blazegraph view
                    (pathPrefix("sparql") & pathEndOrSingleSlash) {
                      concat(
                        // Query
                        ((get & parameter("query".as[SparqlQuery])) | (post & entity(as[SparqlQuery]))) { query =>
                          queryResponseType.apply { responseType =>
                            emit(viewsQuery.query(id, project, query, responseType))
                          }
                        }
                      )
                    },
                    // Fetch a view original source
                    (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(id)) { id =>
                      authorizeRead {
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
          extractCaller { implicit caller =>
            projectRef { project =>
              // if we are on the path /resources/{org}/{proj}/ we need to consume the {schema} segment before consuming the {id}
              consumeIdSegmentIf(segment == "resources") {
                idSegment { id =>
                  incomingOutgoing(id, project)
                }
              }
            }
          }
        }
      )
    }
  }

  private def consumeIdSegmentIf(condition: Boolean): Directive0 =
    if (condition) idSegment.flatMap(_ => pass)
    else pass

  private def incomingOutgoing(id: IdSegment, project: ProjectRef)(implicit caller: Caller) = {
    val authorizeQuery  = authorizeFor(project, Query)
    val metadataContext = ContextValue(Vocabulary.contexts.metadata)
    concat(
      (pathPrefix("incoming") & fromPaginated & pathEndOrSingleSlash & get & extractHttp4sUri) { (pagination, uri) =>
        implicit val searchJsonLdEncoder: JsonLdEncoder[SearchResults[SparqlLink]] =
          searchResultsJsonLdEncoder(metadataContext, pagination, uri)
        authorizeQuery {
          emit(incomingOutgoingLinks.incoming(id, project, pagination))
        }
      },
      (pathPrefix("outgoing") & fromPaginated & pathEndOrSingleSlash & get & extractHttp4sUri & parameter(
        "includeExternalLinks".as[Boolean] ? true
      )) { (pagination, uri, includeExternal) =>
        implicit val searchJsonLdEncoder: JsonLdEncoder[SearchResults[SparqlLink]] =
          searchResultsJsonLdEncoder(metadataContext, pagination, uri)
        authorizeQuery {
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
  )(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering,
      pc: PaginationConfig,
      fusionConfig: FusionConfig
  ): Route = {
    new BlazegraphViewsRoutes(
      views,
      viewsQuery,
      incomingOutgoingLinks,
      identities,
      aclCheck
    ).routes
  }
}
