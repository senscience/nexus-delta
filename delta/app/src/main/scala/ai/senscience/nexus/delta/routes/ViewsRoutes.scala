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
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

final class ViewsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    viewsList: AggregateViewsList
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck: AclCheck) {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      extractCaller { implicit caller =>
        pathPrefix("views") {
          projectRef { project =>
            val authorizeRead                                   = authorizeFor(project, Read)
            given JsonLdEncoder[SearchResults[ResourceF[Unit]]] =
              searchResultsJsonLdEncoder(ContextEmpty)
            (get & authorizeRead & pathEndOrSingleSlash) {
              emit(viewsList(project))
            }
          }
        }
      }
    }
}
