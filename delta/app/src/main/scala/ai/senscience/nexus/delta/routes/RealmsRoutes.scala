package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.RealmResource
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.RealmSearchParams
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.*
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.realms as realmsPermissions
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.realms.model.{Realm, RealmFields, RealmRejection}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.{Directive1, ExceptionHandler, Route}
import cats.effect.IO
import cats.implicits.*

class RealmsRoutes(identities: Identities, realms: Realms, aclCheck: AclCheck)(implicit
    baseUri: BaseUri,
    paginationConfig: PaginationConfig,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  private val exceptionHandler =
    handleExceptions(
      ExceptionHandler { case err: RealmRejection => discardEntityAndForceEmit(err) }
    )

  private def realmsSearchParams: Directive1[RealmSearchParams] =
    searchParams.tmap { case (deprecated, rev, createdBy, updatedBy) =>
      RealmSearchParams(None, deprecated, rev, createdBy, updatedBy)
    }

  private def emitMetadata(statusCode: StatusCode, io: IO[RealmResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[RealmResource]): Route = emitMetadata(StatusCodes.OK, io)

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & exceptionHandler) {
      pathPrefix("realms") {
        extractCaller { implicit caller =>
          concat(
            // List realms
            (get & extractHttp4sUri & fromPaginated & realmsSearchParams & sort[Realm] & pathEndOrSingleSlash) {
              (uri, pagination, params, order) =>
                authorizeFor(AclAddress.Root, realmsPermissions.read).apply {
                  implicit val encoder: JsonLdEncoder[SearchResults[RealmResource]] =
                    searchResultsJsonLdEncoder(Realm.context, pagination, uri)
                  val result                                                        = realms
                    .list(pagination, params, order)
                    .widen[SearchResults[RealmResource]]
                  emit(result)
                }
            },
            (label & pathEndOrSingleSlash) { id =>
              concat(
                // Create or update a realm
                put {
                  authorizeFor(AclAddress.Root, realmsPermissions.write).apply {
                    parameter("rev".as[Int].?) {
                      case Some(rev) =>
                        // Update a realm
                        entity(as[RealmFields]) { fields => emitMetadata(realms.update(id, rev, fields)) }
                      case None      =>
                        // Create a realm
                        entity(as[RealmFields]) { fields =>
                          emitMetadata(StatusCodes.Created, realms.create(id, fields))
                        }
                    }
                  }
                },
                // Fetch a realm
                get {
                  authorizeFor(AclAddress.Root, realmsPermissions.read).apply {
                    parameter("rev".as[Int].?) {
                      case Some(rev) => // Fetch realm at specific revision
                        emit(realms.fetchAt(id, rev))
                      case None      => // Fetch realm
                        emit(realms.fetch(id))
                    }
                  }
                },
                // Deprecate realm
                delete {
                  authorizeFor(AclAddress.Root, realmsPermissions.write).apply {
                    parameter("rev".as[Int]) { rev =>
                      emitMetadata(realms.deprecate(id, rev))
                    }
                  }
                }
              )
            }
          )
        }
      }
    }
}

object RealmsRoutes {

  /**
    * @return
    *   the [[Route]] for realms
    */
  def apply(identities: Identities, realms: Realms, aclCheck: AclCheck)(implicit
      baseUri: BaseUri,
      paginationConfig: PaginationConfig,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Route =
    new RealmsRoutes(identities, realms, aclCheck).routes

}
