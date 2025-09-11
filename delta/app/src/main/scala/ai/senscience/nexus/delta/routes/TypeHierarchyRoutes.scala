package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.typehierarchy
import ai.senscience.nexus.delta.sdk.typehierarchy.TypeHierarchy as TypeHierarchyModel
import ai.senscience.nexus.delta.sdk.typehierarchy.model.{TypeHierarchy, TypeHierarchyRejection}
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, Route}

final class TypeHierarchyRoutes(
    typeHierarchy: TypeHierarchyModel,
    identities: Identities,
    aclCheck: AclCheck
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  private val exceptionHandler = ExceptionHandler { case err: TypeHierarchyRejection =>
    discardEntityAndForceEmit(err)
  }

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & handleExceptions(exceptionHandler)) {
      extractCaller { implicit caller =>
        pathPrefix("type-hierarchy") {
          concat(
            // Fetch using the revision
            (get & parameter("rev".as[Int]) & pathEndOrSingleSlash) { rev =>
              emit(typeHierarchy.fetch(rev))
            },
            // Fetch the type hierarchy
            (get & pathEndOrSingleSlash) {
              emit(typeHierarchy.fetch)
            },
            // Create the type hierarchy
            (post & pathEndOrSingleSlash) {
              entity(as[TypeHierarchy]) { payload =>
                authorizeFor(AclAddress.Root, typehierarchy.write).apply {
                  emit(StatusCodes.Created, typeHierarchy.create(payload.mapping))
                }
              }
            },
            // Update the type hierarchy
            (put & parameter("rev".as[Int]) & pathEndOrSingleSlash) { rev =>
              entity(as[TypeHierarchy]) { payload =>
                authorizeFor(AclAddress.Root, typehierarchy.write).apply {
                  emit(typeHierarchy.update(payload.mapping, rev))
                }
              }
            }
          )
        }
      }
    }

}
