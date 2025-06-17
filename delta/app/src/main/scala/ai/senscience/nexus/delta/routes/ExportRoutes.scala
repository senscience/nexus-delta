package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.baseUriPrefix
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sourcing.exporter.{ExportEventQuery, Exporter}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route

class ExportRoutes(identities: Identities, aclCheck: AclCheck, exporter: Exporter)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("export") {
        pathPrefix("events") {
          extractCaller { implicit caller =>
            (post & pathEndOrSingleSlash & entity(as[ExportEventQuery])) { query =>
              authorizeFor(AclAddress.Root, Permissions.exporter.run).apply {
                emit(StatusCodes.Accepted, exporter.events(query).start.void)
              }
            }
          }
        }
      }
    }

}
