package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.{FetchIndexingView, SparqlRestartScheduler}
import ai.senscience.nexus.delta.plugins.blazegraph.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.Root
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives, ProjectionsDirectives, RouteClassifier, RouteContext}
import ai.senscience.nexus.delta.sdk.directives.RouteClassifier.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

class BlazegraphViewsIndexingRoutes(
    fetch: FetchIndexingView,
    sparqlRestartScheduler: SparqlRestartScheduler,
    identities: Identities,
    aclCheck: AclCheck,
    projectionDirectives: ProjectionsDirectives
)(using ctx: RouteContext, tracer: Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with DeltaDirectives
    with RdfMarshalling
    with BlazegraphViewsDirectives {

  import ctx.given

  private def fetchActiveView =
    (projectRef & idSegment).tflatMap { case (project, idSegment) =>
      onSuccess(fetch(idSegment, project).unsafeToFuture())
    }

  def routes: Route =
    handleExceptions(BlazegraphExceptionHandler.apply) {
      concat(views, jobs)
    }

  private def views =
    pathPrefix("views") {
      extractCaller { case caller =>
        given Subject = caller.subject
        fetchActiveView { view =>
          val project        = view.ref.project
          val authorizeRead  = authorizeFor(project, Read)(using caller)
          val authorizeWrite = authorizeFor(project, Write)(using caller)
          concat(
            // Fetch a blazegraph view statistics
            (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
              authorizeRead {
                projectionDirectives.statistics(project, view.selectFilter, view.projection)
              }
            },
            // Fetch blazegraph view indexing failures
            (pathPrefix("failures") & get) {
              authorizeWrite {
                projectionDirectives.indexingErrors(view.ref)
              }
            },
            // Manage a blazegraph view offset
            (pathPrefix("offset") & pathEndOrSingleSlash) {
              concat(
                // Fetch a blazegraph view offset
                (get & authorizeRead) {
                  projectionDirectives.offset(view.projection)
                },
                // Remove a blazegraph view offset (restart the view)
                (delete & authorizeWrite & offset("from")) { fromOffset =>
                  projectionDirectives.scheduleRestart(view.projectionMetadata, fromOffset)
                }
              )
            },
            // Getting indexing status for a resource in the given view
            (pathPrefix("status") & authorizeRead) {
              projectionDirectives.indexingStatus(project, view.selectFilter, view.projection, IO.unit)
            }
          )
        }
      }
    }

  private def jobs =
    (pathPrefix("jobs") & pathPrefix("sparql") & pathPrefix("reindex")) {
      extractCaller { case caller =>
        val authorizeRootWrite = authorizeFor(Root, Write)(using caller)
        given Subject          = caller.subject
        (post & authorizeRootWrite & offset("from") & pathEndOrSingleSlash) { offset =>
          emit(
            StatusCodes.Accepted,
            sparqlRestartScheduler.run(offset).start.void
          )
        }
      }
    }
}

object BlazegraphViewsIndexingRoutes {

  /** Names the Blazegraph views indexing routes for tracing, mirroring the route tree. */
  val classifier: RouteClassifier = RouteClassifier(
    route("views" / str("org") / str("project"))(
      route(str("id"))(
        route("statistics"),
        route("failures"),
        route("offset"),
        route("status")
      )
    )
  )

  /**
    * @return
    *   the [[Route]] for BlazegraphViews
    */
  def apply(
      fetch: FetchIndexingView,
      sparqlRestartScheduler: SparqlRestartScheduler,
      identities: Identities,
      aclCheck: AclCheck,
      projectionDirectives: ProjectionsDirectives
  )(using RouteContext, Tracer[IO]): Route = {
    new BlazegraphViewsIndexingRoutes(
      fetch,
      sparqlRestartScheduler,
      identities,
      aclCheck,
      projectionDirectives
    ).routes
  }
}
