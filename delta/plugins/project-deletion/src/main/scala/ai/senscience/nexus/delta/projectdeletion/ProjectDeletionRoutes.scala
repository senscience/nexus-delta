package ai.senscience.nexus.delta.projectdeletion

import ai.senscience.nexus.delta.projectdeletion.model.ProjectDeletionConfig
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

/**
  * The project deletion routes that expose the current configuration of the plugin.
  *
  * @param config
  *   the automatic project deletion configuration
  */
class ProjectDeletionRoutes(config: ProjectDeletionConfig)(using baseUri: BaseUri)(using
    RemoteContextResolution,
    JsonKeyOrdering,
    Tracer[IO]
) extends RdfMarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("project-deletion" / "config") {
        emit(IO.pure(config))
      }
    }

}
