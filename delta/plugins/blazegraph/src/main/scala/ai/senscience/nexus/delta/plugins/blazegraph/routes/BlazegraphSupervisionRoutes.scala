package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.supervision.{BlazegraphViewByNamespace, SparqlSupervision}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.emitJson
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.permissions.Permissions.supervision
import akka.http.scaladsl.server.Route

class BlazegraphSupervisionRoutes(
    blazegraphSupervision: SparqlSupervision,
    identities: Identities,
    aclCheck: AclCheck
)(implicit ordering: JsonKeyOrdering)
    extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {

  def routes: Route =
    pathPrefix("supervision") {
      extractCaller { implicit caller =>
        authorizeFor(AclAddress.Root, supervision.read).apply {
          (pathPrefix("blazegraph") & get & pathEndOrSingleSlash) {
            emitJson(blazegraphSupervision.get)
          }
        }
      }
    }
}

object BlazegraphSupervisionRoutes {

  def apply(views: BlazegraphViews, client: SparqlClient, identities: Identities, aclCheck: AclCheck)(implicit
      ordering: JsonKeyOrdering
  ): BlazegraphSupervisionRoutes = {
    val viewsByNameSpace      = BlazegraphViewByNamespace(views)
    val blazegraphSupervision = SparqlSupervision(client, viewsByNameSpace)
    new BlazegraphSupervisionRoutes(blazegraphSupervision, identities, aclCheck)
  }

}
