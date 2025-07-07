package ai.senscience.nexus.delta.plugins.elasticsearch.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.plugins.elasticsearch.client.PointInTime
import ai.senscience.nexus.delta.plugins.elasticsearch.model.ViewResource
import ai.senscience.nexus.delta.plugins.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.elasticsearch.{ElasticSearchViews, ElasticSearchViewsQuery}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.{OriginalSource, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.*
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.*
import cats.effect.IO
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

/**
  * The elasticsearch views routes
  *
  * @param identities
  *   the identity module
  * @param aclCheck
  *   to check acls
  * @param views
  *   the elasticsearch views operations bundle
  * @param viewsQuery
  *   the elasticsearch views query operations bundle
  */
final class ElasticSearchViewsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    views: ElasticSearchViews,
    viewsQuery: ElasticSearchViewsQuery
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering,
    fusionConfig: FusionConfig
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with ElasticSearchViewsDirectives
    with RdfMarshalling {

  private def emitMetadataOrReject(statusCode: StatusCode, io: IO[ViewResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadataOrReject(io: IO[ViewResource]): Route = emitMetadataOrReject(StatusCodes.OK, io)

  private def emitSource(io: IO[ViewResource]): Route =
    emit(io.map { resource => OriginalSource(resource, resource.value.source) })

  def routes: Route = {
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { implicit caller =>
          projectRef { project =>
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              pathEndOrSingleSlash {
                // Create an elasticsearch view without id segment
                (post & pathEndOrSingleSlash & noParameter("rev") & entity(as[Json])) { source =>
                  authorizeWrite {
                    emitMetadataOrReject(Created, views.create(project, source))
                  }
                }
              },
              idSegment { id =>
                concat(
                  pathEndOrSingleSlash {
                    concat(
                      // Create or update an elasticsearch view
                      put {
                        authorizeWrite {
                          (parameter("rev".as[Int].?) & pathEndOrSingleSlash & entity(as[Json])) {
                            case (None, source)      =>
                              // Create an elasticsearch view with id segment
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
                      // Deprecate an elasticsearch view
                      (delete & parameter("rev".as[Int])) { rev =>
                        authorizeWrite {
                          emitMetadataOrReject(views.deprecate(id, project, rev))
                        }
                      },
                      // Fetch an elasticsearch view
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
                  // Undeprecate an elasticsearch view
                  (pathPrefix("undeprecate") & put & pathEndOrSingleSlash & parameter("rev".as[Int])) { rev =>
                    authorizeWrite {
                      emitMetadataOrReject(
                        views.undeprecate(id, project, rev)
                      )
                    }
                  },
                  // Query an elasticsearch view
                  (pathPrefix("_search") & post & pathEndOrSingleSlash) {
                    (extractQueryParams & entity(as[JsonObject])) { (qp, query) =>
                      emit(viewsQuery.query(id, project, query, qp))
                    }
                  },
                  // Create a point in time for the given view
                  (pathPrefix("_pit") & parameter("keep_alive".as[Long]) & post & pathEndOrSingleSlash) { keepAlive =>
                    val keepAliveDuration = Duration(keepAlive, TimeUnit.SECONDS)
                    emit(
                      viewsQuery
                        .createPointInTime(id, project, keepAliveDuration)
                        .map(_.asJson)
                    )
                  },
                  // Delete a point in time
                  (pathPrefix("_pit") & entity(as[PointInTime]) & delete & pathEndOrSingleSlash) { pit =>
                    emit(
                      StatusCodes.NoContent,
                      viewsQuery.deletePointInTime(pit)
                    )
                  },
                  // Fetch an elasticsearch view original source
                  (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(id)) { id =>
                    authorizeFor(project, Read).apply {
                      emitSource(views.fetch(id, project))
                    }
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

object ElasticSearchViewsRoutes {

  /**
    * @return
    *   the [[Route]] for elasticsearch views
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      views: ElasticSearchViews,
      viewsQuery: ElasticSearchViewsQuery
  )(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering,
      fusionConfig: FusionConfig
  ): Route =
    new ElasticSearchViewsRoutes(
      identities,
      aclCheck,
      views,
      viewsQuery
    ).routes
}
