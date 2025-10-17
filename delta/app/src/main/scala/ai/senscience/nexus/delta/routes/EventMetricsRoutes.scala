package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.elasticsearch.metrics.EventMetricsProjection
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.supervision
import ai.senscience.nexus.delta.sourcing.Scope.Root
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

final class EventMetricsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    projectionsDirectives: ProjectionsDirectives
)(using baseUri: BaseUri)(using Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with DeltaDirectives
    with RdfMarshalling {

  private val projectionName = EventMetricsProjection.projectionMetadata.name

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("event-metrics") {
        extractCaller { implicit caller =>
          authorizeFor(AclAddress.Root, supervision.read).apply {
            concat(
              pathPrefix("statistics") {
                projectionsDirectives.statistics(Root, SelectFilter.latest, projectionName)
              },
              pathPrefix("failures") {
                projectionsDirectives.indexingErrors(projectionName)
              },
              (pathPrefix("offset") & pathEndOrSingleSlash) {
                concat(
                  // Fetch the event metrics projection offset
                  get {
                    projectionsDirectives.offset(projectionName)
                  },
                  // Delete the event metrics project offset (restart it)
                  (delete & offset("from")) { fromOffset =>
                    projectionsDirectives.scheduleRestart(projectionName, fromOffset)
                  }
                )
              }
            )
          }
        }
      }
    }

}
