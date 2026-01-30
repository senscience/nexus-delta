package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaSchemeDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.instances.given
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.events
import ai.senscience.nexus.delta.sdk.sse.SseElemStream
import ai.senscience.nexus.delta.sourcing.model.Tag.Latest
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.RemainingElems
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes.OK
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

import java.time.Instant

/**
  * Route to stream elems as SSEs
  *
  * Note that this endpoint is experimental, susceptible to changes or removal
  */
class ElemRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    sseElemStream: SseElemStream,
    schemeDirectives: DeltaSchemeDirectives
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck: AclCheck) {
  import schemeDirectives.*

  private val tagParamOrLatest = tagParam.map(_.getOrElse(Latest))

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      extractCaller { case given Caller =>
        lastEventId { offset =>
          pathPrefix("elems") {
            projectRef { project =>
              authorizeFor(project, events.read).apply {
                (tagParamOrLatest & types(project)) { (tag, types) =>
                  concat(
                    (get & pathPrefix("continuous")) {
                      emit(sseElemStream.continuous(project, SelectFilter(types, tag), offset))
                    },
                    (get & pathPrefix("currents")) {
                      emit(sseElemStream.currents(project, SelectFilter(types, tag), offset))
                    },
                    (get & pathPrefix("remaining")) {
                      emit(
                        sseElemStream
                          .remaining(project, SelectFilter(types, tag), offset)
                          .map { _.getOrElse(RemainingElems(0L, Instant.EPOCH)) }
                      )
                    },
                    head {
                      complete(OK)
                    }
                  )
                }
              }
            }
          }
        }
      }
    }
}
