package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.SparqlSlowQueryLogger
import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.model.SparqlSlowQuery.{sparqlSlowQueryJsonLdEncoder, SparqlSlowQueryResults}
import ai.senscience.nexus.delta.plugins.blazegraph.supervision.SparqlSupervision
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import org.apache.pekko.http.scaladsl.server.Route

class SparqlSupervisionRoutes(
    supervision: SparqlSupervision,
    slowQueryLogger: SparqlSlowQueryLogger,
    identities: Identities,
    aclCheck: AclCheck
)(implicit baseUri: BaseUri, cr: RemoteContextResolution, ordering: JsonKeyOrdering)
    extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {

  implicit private val paginationConfig: PaginationConfig = PaginationConfig(50, 1_000, 10_000)

  def routes: Route =
    pathPrefix("supervision") {
      extractCaller { implicit caller =>
        authorizeFor(AclAddress.Root, Permissions.supervision.read).apply {
          pathPrefix("blazegraph") {
            concat(
              (get & pathEndOrSingleSlash) {
                emitJson(supervision.get)
              },
              (pathPrefix("slow-queries") & get & fromPaginated & timeRange(
                "instant"
              ) & extractHttp4sUri & pathEndOrSingleSlash) { (pagination, timeRange, uri) =>
                implicit val searchJsonLdEncoder: JsonLdEncoder[SparqlSlowQueryResults] =
                  sparqlSlowQueryJsonLdEncoder(pagination, uri)
                emit(slowQueryLogger.search(pagination, timeRange))
              }
            )
          }
        }
      }
    }
}
