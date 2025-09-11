package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.elasticsearch.model.permissions.read as Read
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextEmpty
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.views.ViewsList.AggregateViewsList
import org.apache.pekko.http.scaladsl.server.Route

final class ViewsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    viewsList: AggregateViewsList
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck: AclCheck) {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      extractCaller { implicit caller =>
        pathPrefix("views") {
          projectRef { project =>
            val authorizeRead                                                               = authorizeFor(project, Read)
            implicit val searchJsonLdEncoder: JsonLdEncoder[SearchResults[ResourceF[Unit]]] =
              searchResultsJsonLdEncoder(ContextEmpty)
            (get & authorizeRead & pathEndOrSingleSlash) {
              emit(viewsList(project))
            }
          }
        }
      }
    }

}
