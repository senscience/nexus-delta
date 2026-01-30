package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

/**
  * The identities routes
  */
class IdentitiesRoutes(identities: Identities, aclCheck: AclCheck)(using baseUri: BaseUri)(using
    RemoteContextResolution,
    JsonKeyOrdering,
    Tracer[IO]
) extends AuthDirectives(identities, aclCheck) {

  def routes: Route = {
    baseUriPrefix(baseUri.prefix) {
      (pathPrefix("identities") & pathEndOrSingleSlash) {
        (extractCaller & get) { caller =>
          emit(IO.pure(caller))
        }
      }
    }
  }
}

object IdentitiesRoutes {

  /**
    * @return
    *   the [[Route]] for identities
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): Route =
    new IdentitiesRoutes(identities, aclCheck).routes
}
