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
import akka.http.scaladsl.server.Route

final class EventMetricsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    projectionsDirectives: ProjectionsDirectives
)(implicit baseUri: BaseUri)
    extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("event-metrics") {
        extractCaller { implicit caller =>
          pathPrefix("failures") {
            authorizeFor(AclAddress.Root, supervision.read).apply {
              projectionsDirectives.indexingErrors(EventMetricsProjection.projectionMetadata.name)
            }
          }
        }
      }
    }

}
