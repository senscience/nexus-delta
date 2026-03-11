package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.resources.ResourcesExporter
import ai.senscience.nexus.delta.sourcing.exporter.{ExportEventQuery, Exporter}
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

class ExportRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    exporter: Exporter,
    resourcesExporter: ResourcesExporter
)(using
    baseUri: BaseUri
)(using
    RemoteContextResolution,
    JsonKeyOrdering,
    Tracer[IO]
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("export") {
        extractCaller { case given Caller =>
          authorizeFor(AclAddress.Root, Permissions.exporter.run).apply {
            pathPrefix("events") {
              (post & pathEndOrSingleSlash & entity(as[ExportEventQuery])) { query =>
                emit(StatusCodes.Accepted, exporter.events(query).start.void)
              }
            } ~
              pathPrefix("resources") {
                (post & pathEndOrSingleSlash & updatedAt) { timeRange =>
                  emit(StatusCodes.Accepted, resourcesExporter.exportAll(timeRange).start.void)
                } ~
                  (post & projectRef & pathEndOrSingleSlash) { project =>
                    emit(StatusCodes.Accepted, resourcesExporter.exportProject(project).start.void)
                  }
              }
          }
        }
      }
    }

}
