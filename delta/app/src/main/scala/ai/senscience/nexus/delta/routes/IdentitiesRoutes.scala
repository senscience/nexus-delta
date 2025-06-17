package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import akka.http.scaladsl.server.Route
import cats.effect.IO

/**
  * The identities routes
  */
class IdentitiesRoutes(identities: Identities, aclCheck: AclCheck)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck) {

  def routes: Route = {
    baseUriPrefix(baseUri.prefix) {
      (pathPrefix("identities") & pathEndOrSingleSlash) {
        (extractCaller & get) { implicit caller =>
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
  )(implicit baseUri: BaseUri, cr: RemoteContextResolution, ordering: JsonKeyOrdering): Route =
    new IdentitiesRoutes(identities, aclCheck).routes
}
