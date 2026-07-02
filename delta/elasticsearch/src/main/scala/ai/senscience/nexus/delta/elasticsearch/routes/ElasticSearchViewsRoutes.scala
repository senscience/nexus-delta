package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.client.PointInTime
import ai.senscience.nexus.delta.elasticsearch.model.ViewResource
import ai.senscience.nexus.delta.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchViews, ElasticSearchViewsQuery}
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.RouteClassifier
import ai.senscience.nexus.delta.sdk.directives.RouteClassifier.*
import ai.senscience.nexus.delta.sdk.directives.RouteContext
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes.Created
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

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
)(using ctx: RouteContext, tracer: Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with ElasticSearchViewsDirectives
    with RdfMarshalling {

  import ctx.given

  private def emitMetadataOrReject(statusCode: StatusCode, io: IO[ViewResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadataOrReject(io: IO[ViewResource]): Route = emitMetadataOrReject(StatusCodes.OK, io)

  private def emitSource(io: IO[ViewResource]): Route =
    emit(io.map { resource => OriginalSource(resource, resource.value.source) })

  def routes: Route = {
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { caller =>
          given Caller  = caller
          given Subject = caller.subject
          projectRef { project =>
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              // Create an elasticsearch view without id segment
              (post & pathEndOrSingleSlash & noRev & authorizeWrite & jsonEntity) { source =>
                emitMetadataOrReject(Created, views.create(project, source))
              },
              idSegment { id =>
                concat(
                  pathEndOrSingleSlash {
                    concat(
                      // Create or update an elasticsearch view
                      (put & authorizeWrite & revParamOpt & pathEndOrSingleSlash & jsonEntity) {
                        case (None, source)      =>
                          // Create an elasticsearch view with id segment
                          emitMetadataOrReject(Created, views.create(id, project, source))
                        case (Some(rev), source) =>
                          // Update a view
                          emitMetadataOrReject(views.update(id, project, rev, source))
                      },
                      // Deprecate an elasticsearch view
                      (delete & authorizeWrite & revParam) { rev =>
                        emitMetadataOrReject(views.deprecate(id, project, rev))
                      },
                      // Fetch an elasticsearch view
                      (get & idSegmentRef(id)) { id =>
                        val fetchRoute = authorizeRead { emit(views.fetch(id, project)) }
                        emitOrFusionRedirect(project, id, fetchRoute)
                      }
                    )
                  },
                  // Undeprecate an elasticsearch view
                  (pathPrefix("undeprecate") & put & authorizeWrite & pathEndOrSingleSlash & revParam) { rev =>
                    emitMetadataOrReject(
                      views.undeprecate(id, project, rev)
                    )
                  },
                  // Query an elasticsearch view
                  (pathPrefix("_search") & post & pathEndOrSingleSlash) {
                    elasticSearchRequest { request =>
                      emit(viewsQuery.query(id, project, request))
                    }
                  },
                  // Create a point in time for the given view
                  (pathPrefix("_pit") & parameter("keep_alive".as[Long]) & post & pathEndOrSingleSlash) { keepAlive =>
                    val keepAliveDuration = Duration(keepAlive, TimeUnit.SECONDS)
                    emitJson(viewsQuery.createPointInTime(id, project, keepAliveDuration))
                  },
                  // Delete a point in time
                  (pathPrefix("_pit") & entity(as[PointInTime]) & delete & pathEndOrSingleSlash) { pit =>
                    emit(StatusCodes.NoContent, viewsQuery.deletePointInTime(pit))
                  },
                  // Fetch an elasticsearch view original source
                  (pathPrefix("source") & get & authorizeRead & pathEndOrSingleSlash & idSegmentRef(id)) { id =>
                    emitSource(views.fetch(id, project))
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

  /** Names the elasticsearch views routes for tracing, mirroring the route tree. */
  val classifier: RouteClassifier = RouteClassifier(
    route("views" / str("org") / str("project"))(
      route(str("id"))(
        route("undeprecate"),
        route("_search"),
        route("_pit"),
        route("source")
      )
    )
  )

  /**
    * @return
    *   the [[Route]] for elasticsearch views
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      views: ElasticSearchViews,
      viewsQuery: ElasticSearchViewsQuery
  )(using RouteContext, Tracer[IO]): Route =
    new ElasticSearchViewsRoutes(
      identities,
      aclCheck,
      views,
      viewsQuery
    ).routes
}
