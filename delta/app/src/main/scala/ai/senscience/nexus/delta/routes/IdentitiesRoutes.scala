package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, RouteContext}
import ai.senscience.nexus.delta.sdk.identities.Identities
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

/**
  * The identities routes
  */
class IdentitiesRoutes(identities: Identities, aclCheck: AclCheck)(using ctx: RouteContext, tracer: Tracer[IO])
    extends AuthDirectives(identities, aclCheck) {

  import ctx.given

  def routes: Route = {
    baseUriPrefix(ctx.baseUri.prefix) {
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
  )(using RouteContext, Tracer[IO]): Route =
    new IdentitiesRoutes(identities, aclCheck).routes
}
