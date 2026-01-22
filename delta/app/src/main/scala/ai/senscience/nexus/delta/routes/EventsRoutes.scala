package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{emit, lastEventId}
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.events
import ai.senscience.nexus.delta.sdk.sse.SseEventLog
import ai.senscience.nexus.delta.sourcing.model.Label
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes.OK
import org.apache.pekko.http.scaladsl.server.{Directive1, Route}
import org.typelevel.otel4s.trace.Tracer

/**
  * The global events route.
  *
  * @param identities
  *   the identities operations bundle
  * @param aclCheck
  *   verify the acls for users
  * @param sseEventLog
  *   the event log
  */
class EventsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    sseEventLog: SseEventLog
)(using baseUri: BaseUri)
    extends AuthDirectives(identities, aclCheck: AclCheck) {

  given Tracer[IO] = Tracer.noop

  private def resolveSelector: Directive1[Label] =
    label.flatMap { l =>
      if sseEventLog.selectors.contains(l) then provide(l)
      else reject()
    }

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      extractCaller { case given Caller =>
        lastEventId { offset =>
          concat(
            concat(
              // SSE for all events with a given selector
              (resolveSelector & pathPrefix("events") & pathEndOrSingleSlash) { selector =>
                authorizeFor(AclAddress.Root, events.read).apply {
                  concat(
                    get { emit(sseEventLog.streamBy(selector, offset)) },
                    head { complete(OK) }
                  )
                }

              },
              // SSE for events with a given selector within a given organization
              (resolveSelector & label & pathPrefix("events") & pathEndOrSingleSlash) { (selector, org) =>
                authorizeFor(org, events.read).apply {
                  concat(
                    get { emit(sseEventLog.streamBy(selector, org, offset)) },
                    head { complete(OK) }
                  )
                }
              },
              // SSE for events with a given selector within a given project
              (resolveSelector & projectRef & pathPrefix("events") & pathEndOrSingleSlash) { (selector, project) =>
                authorizeFor(project, events.read) {
                  concat(
                    get { emit(sseEventLog.streamBy(selector, project, offset)) },
                    head { complete(OK) }
                  )
                }
              }
            )
          )
        }
      }
    }

}

object EventsRoutes {

  /**
    * @return
    *   [[Route]] for events.
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      sseEventLog: SseEventLog
  )(using BaseUri): Route = new EventsRoutes(identities, aclCheck, sseEventLog).routes

}
