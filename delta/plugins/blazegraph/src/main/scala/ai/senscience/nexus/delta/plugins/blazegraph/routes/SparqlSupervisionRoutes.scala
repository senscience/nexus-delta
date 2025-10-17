package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.supervision.SparqlSupervision
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

class SparqlSupervisionRoutes(
    supervision: SparqlSupervision,
    identities: Identities,
    aclCheck: AclCheck
)(using JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {

  def routes: Route =
    pathPrefix("supervision") {
      extractCaller { implicit caller =>
        authorizeFor(AclAddress.Root, Permissions.supervision.read).apply {
          pathPrefix("blazegraph") {
            concat(
              (get & pathEndOrSingleSlash) {
                emitJson(supervision.get)
              }
            )
          }
        }
      }
    }
}
