package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.RouteContext
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.permissions.Permissions.typehierarchy
import ai.senscience.nexus.delta.sdk.typehierarchy.TypeHierarchy as TypeHierarchyModel
import ai.senscience.nexus.delta.sdk.typehierarchy.model.{TypeHierarchy, TypeHierarchyRejection}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, Route}
import org.typelevel.otel4s.trace.Tracer

final class TypeHierarchyRoutes(
    typeHierarchy: TypeHierarchyModel,
    identities: Identities,
    aclCheck: AclCheck
)(using ctx: RouteContext, tracer: Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  import ctx.given

  private val exceptionHandler = ExceptionHandler { case err: TypeHierarchyRejection =>
    discardEntityAndForceEmit(err)
  }

  private val typeHierarchyMapping = entity(as[TypeHierarchy]).map(_.mapping)

  def routes: Route =
    (baseUriPrefix(ctx.baseUri.prefix) & handleExceptions(exceptionHandler)) {
      extractCaller { case caller =>
        val authorizeWrite = authorizeFor(AclAddress.Root, typehierarchy.write)(using caller)
        given Subject      = caller.subject
        pathPrefix("type-hierarchy") {
          concat(
            // Fetch using the revision
            (get & revParam & pathEndOrSingleSlash) { rev =>
              emit(typeHierarchy.fetch(rev))
            },
            // Fetch the type hierarchy
            (get & pathEndOrSingleSlash) {
              emit(typeHierarchy.fetch)
            },
            // Create the type hierarchy
            (post & pathEndOrSingleSlash & authorizeWrite & typeHierarchyMapping) { mapping =>
              emit(StatusCodes.Created, typeHierarchy.create(mapping))
            },
            // Update the type hierarchy
            (put & pathEndOrSingleSlash & authorizeWrite & typeHierarchyMapping & revParam) { case (mapping, rev) =>
              emit(typeHierarchy.update(mapping, rev))
            }
          )
        }
      }
    }
}
