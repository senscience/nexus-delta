package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.elasticsearch.metrics.EventMetricsProjection
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.baseUriPrefix
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.supervision
import ai.senscience.nexus.delta.sourcing.Scope.Root
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import akka.http.scaladsl.server.Route

final class EventMetricsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    projectionsDirectives: ProjectionsDirectives
)(implicit baseUri: BaseUri)
    extends AuthDirectives(identities, aclCheck)
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
              }
            )
          }
        }
      }
    }

}
