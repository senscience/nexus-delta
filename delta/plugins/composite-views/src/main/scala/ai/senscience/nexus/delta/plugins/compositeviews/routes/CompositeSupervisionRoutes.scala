package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.supervision.SparqlSupervision
import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViews
import ai.senscience.nexus.delta.plugins.compositeviews.supervision.CompositeViewsByNamespace
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.emit
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.permissions.Permissions.supervision
import akka.http.scaladsl.server.Route
import io.circe.syntax.EncoderOps

class CompositeSupervisionRoutes(
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
          (pathPrefix("composite-views") & get & pathEndOrSingleSlash) {
            emit(blazegraphSupervision.get.map(_.asJson))
          }
        }
      }
    }
}

object CompositeSupervisionRoutes {
  def apply(
      views: CompositeViews,
      client: SparqlClient,
      identities: Identities,
      aclCheck: AclCheck,
      prefix: String
  )(implicit ordering: JsonKeyOrdering): CompositeSupervisionRoutes = {
    val viewsByNameSpace     = CompositeViewsByNamespace(views, prefix)
    val compositeSupervision = SparqlSupervision(client, viewsByNameSpace)
    new CompositeSupervisionRoutes(compositeSupervision, identities, aclCheck)
  }
}
