package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{emit, lastEventId}
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.events
import ai.senscience.nexus.delta.sdk.sse.SseEventLog
import ai.senscience.nexus.delta.sourcing.model.Label
import org.apache.pekko.http.scaladsl.model.StatusCodes.OK
import org.apache.pekko.http.scaladsl.server.{Directive1, Route}

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
)(implicit baseUri: BaseUri)
    extends AuthDirectives(identities, aclCheck: AclCheck) {

  private def resolveSelector: Directive1[Label] =
    label.flatMap { l =>
      if (sseEventLog.selectors.contains(l))
        provide(l)
      else
        reject()
    }

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      extractCaller { implicit caller =>
        lastEventId { offset =>
          concat(
            concat(
              // SSE for all events with a given selector
              (resolveSelector & pathPrefix("events") & pathEndOrSingleSlash & get) { selector =>
                concat(
                  authorizeFor(AclAddress.Root, events.read).apply {
                    emit(sseEventLog.streamBy(selector, offset))
                  },
                  (head & authorizeFor(AclAddress.Root, events.read)) {
                    complete(OK)
                  }
                )
              },
              // SSE for events with a given selector within a given organization
              (resolveSelector & label & pathPrefix("events") & pathEndOrSingleSlash & get) { (selector, org) =>
                concat(
                  authorizeFor(org, events.read).apply {
                    emit(sseEventLog.streamBy(selector, org, offset))
                  },
                  (head & authorizeFor(org, events.read)) {
                    complete(OK)
                  }
                )
              },
              // SSE for events with a given selector within a given project
              (resolveSelector & projectRef & pathPrefix("events") & pathEndOrSingleSlash) { (selector, project) =>
                concat(
                  (get & authorizeFor(project, events.read)).apply {
                    emit(sseEventLog.streamBy(selector, project, offset))
                  },
                  (head & authorizeFor(project, events.read)) {
                    complete(OK)
                  }
                )
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
  )(implicit baseUri: BaseUri): Route = new EventsRoutes(identities, aclCheck, sseEventLog).routes

}
