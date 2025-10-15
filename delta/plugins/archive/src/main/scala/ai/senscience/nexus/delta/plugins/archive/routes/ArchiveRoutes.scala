package ai.senscience.nexus.delta.plugins.archive.routes

import ai.senscience.nexus.delta.plugins.archive.Archives
import ai.senscience.nexus.delta.plugins.archive.{permissions, ArchiveResource}
import ai.senscience.nexus.delta.plugins.archive.model.{ArchiveRejection, Zip}
import ai.senscience.nexus.delta.plugins.storage.storages.StoragePluginExceptionHandler.handleStorageExceptions
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.FileResponse.PekkoSource
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, FileResponse}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import io.circe.Json
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes.{Created, SeeOther}
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, Route}

/**
  * The Archive routes.
  *
  * @param archives
  *   the archive module
  * @param identities
  *   the identities module
  * @param aclCheck
  *   to check acls
  */
class ArchiveRoutes(
    archives: Archives,
    identities: Identities,
    aclCheck: AclCheck
)(implicit baseUri: BaseUri, rcr: RemoteContextResolution, jko: JsonKeyOrdering)
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  private val archivesExceptionHandler = handleExceptions {
    ExceptionHandler { case err: ArchiveRejection =>
      discardEntityAndForceEmit(err)
    }
  }

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & handleStorageExceptions & archivesExceptionHandler) {
      pathPrefix("archives") {
        extractCaller { implicit caller =>
          projectRef { implicit project =>
            concat(
              // create an archive without an id
              (post & entity(as[Json]) & pathEndOrSingleSlash) { json =>
                authorizeFor(project, permissions.write).apply {
                  emitCreatedArchive(archives.create(project, json))
                }
              },
              (idSegment & pathEndOrSingleSlash) { id =>
                concat(
                  // create an archive with an id
                  (put & entity(as[Json]) & pathEndOrSingleSlash) { json =>
                    authorizeFor(project, permissions.write).apply {
                      emitCreatedArchive(archives.create(id, project, json))
                    }
                  },
                  // fetch or download an archive
                  (get & pathEndOrSingleSlash) {
                    authorizeFor(project, permissions.read).apply {
                      emitArchiveDownload(id, project)
                    }
                  }
                )
              }
            )
          }
        }
      }
    }

  private def emitMetadata(statusCode: StatusCode, io: IO[ArchiveResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitCreatedArchive(io: IO[ArchiveResource]): Route =
    Zip.checkHeader {
      case true  => emitRedirect(SeeOther, io.map(_.access.uri))
      case false => emitMetadata(Created, io)
    }

  private def emitArchiveDownload(id: IdSegment, project: ProjectRef)(implicit caller: Caller): Route =
    Zip.checkHeader {
      case true  =>
        parameter("ignoreNotFound".as[Boolean] ? false) { ignoreNotFound =>
          emitArchiveFile(archives.download(id, project, ignoreNotFound))
        }
      case false => emit(archives.fetch(id, project))
    }

  private def emitArchiveFile(source: IO[PekkoSource]) = {
    val response = source.map { s => FileResponse.noCache(s"archive.zip", Zip.contentType, None, s) }
    emit(response)
  }
}
