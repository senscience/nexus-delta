package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.RealmResource
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.RealmSearchParams
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.*
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.realms as realmsPermissions
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.realms.model.{Realm, RealmFields, RealmRejection}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.syntax.all.*
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.http.scaladsl.server.{Directive1, ExceptionHandler, Route}
import org.typelevel.otel4s.trace.Tracer

class RealmsRoutes(identities: Identities, realms: Realms, aclCheck: AclCheck)(using baseUri: BaseUri)(using
    PaginationConfig,
    RemoteContextResolution,
    JsonKeyOrdering,
    Tracer[IO]
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

  private def emitMetadata(statusCode: StatusCode, io: IO[RealmResource]): Route = {
    import ai.senscience.nexus.delta.sdk.implicits.*
    emit(statusCode, io.mapValue(_.metadata))
  }

  private def emitMetadata(io: IO[RealmResource]): Route = emitMetadata(StatusCodes.OK, io)

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & exceptionHandler) {
      pathPrefix("realms") {
        extractCaller { case caller @ given Caller =>
          given Subject = caller.subject
          concat(
            // List realms
            (get & extractHttp4sUri & fromPaginated & realmsSearchParams & sort[Realm] & pathEndOrSingleSlash) {
              (uri, pagination, params, order) =>
                authorizeFor(AclAddress.Root, realmsPermissions.read).apply {
                  given JsonLdEncoder[SearchResults[RealmResource]] =
                    searchResultsJsonLdEncoder(Realm.context, pagination, uri)
                  emit(realms.list(pagination, params, order).widen[SearchResults[RealmResource]])
                }
            },
            (label & pathEndOrSingleSlash) { id =>
              val authorizeRead  = authorizeFor(AclAddress.Root, realmsPermissions.read)
              val authorizeWrite = authorizeFor(AclAddress.Root, realmsPermissions.write)
              concat(
                // Create or update a realm
                (put & authorizeWrite & entity(as[RealmFields]) & revParamOpt) {
                  case (fields, Some(rev)) =>
                    // Update a realm
                    emitMetadata(realms.update(id, rev, fields))
                  case (fields, None)      =>
                    // Create a realm
                    emitMetadata(StatusCodes.Created, realms.create(id, fields))
                },
                // Fetch a realm
                (get & authorizeRead & revParamOpt) {
                  case Some(rev) => // Fetch realm at specific revision
                    emit(realms.fetchAt(id, rev))
                  case None      => // Fetch realm
                    emit(realms.fetch(id))
                },
                // Deprecate realm
                (delete & authorizeWrite & revParam) { rev =>
                  emitMetadata(realms.deprecate(id, rev))
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
  def apply(identities: Identities, realms: Realms, aclCheck: AclCheck)(using
      BaseUri,
      PaginationConfig,
      RemoteContextResolution,
      JsonKeyOrdering,
      Tracer[IO]
  ): Route =
    new RealmsRoutes(identities, realms, aclCheck).routes

}
