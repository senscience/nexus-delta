package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling.{jsonCodecDropNull, jsonSourceCodec}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceRepresentation}
import ai.senscience.nexus.delta.sdk.multifetch.MultiFetch
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchRequest
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import io.circe.Json
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

/**
  * Route allowing to fetch multiple resources in a single request
  */
class MultiFetchRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    multiFetch: MultiFetch
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("multi-fetch") {
        pathPrefix("resources") {
          extractCaller { implicit caller =>
            ((get | post) & entity(as[MultiFetchRequest])) { request =>
              given JsonValueCodec[Json] = selectCodec(request)
              emit(multiFetch(request).flatMap(_.asJson))
            }
          }
        }
      }
    }

  private def selectCodec(request: MultiFetchRequest) =
    if request.format == ResourceRepresentation.SourceJson ||
      request.format == ResourceRepresentation.AnnotatedSourceJson
    then jsonSourceCodec
    else jsonCodecDropNull

}
