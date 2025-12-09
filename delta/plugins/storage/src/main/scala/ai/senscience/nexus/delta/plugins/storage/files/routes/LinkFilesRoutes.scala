package ai.senscience.nexus.delta.plugins.storage.files.routes

import ai.senscience.nexus.delta.plugins.storage.files.model.{File, FileId, FileLinkRequest}
import ai.senscience.nexus.delta.plugins.storage.files.routes.FileUriDirectives.storageParam
import ai.senscience.nexus.delta.plugins.storage.files.{FileResource, Files}
import ai.senscience.nexus.delta.plugins.storage.storages.StoragePluginExceptionHandler.handleStorageExceptions
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.ShowFileLocation
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.indexing.{IndexingMode, SyncIndexingAction}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.StatusCodes.Created
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

class LinkFilesRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    files: Files,
    index: SyncIndexingAction.Execute[File]
)(using
    baseUri: BaseUri
)(using RemoteContextResolution, JsonKeyOrdering, ShowFileLocation, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {
  self =>

  private def onCreationDirective =
    noRev & storageParam & tagParam & indexingMode & pathEndOrSingleSlash & entity(as[FileLinkRequest])

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      (pathPrefix("link" / "files") & handleStorageExceptions) {
        extractCaller { implicit caller =>
          projectRef { project =>
            implicit class IndexOps(io: IO[FileResource]) {
              def index(m: IndexingMode): IO[FileResource] = io.flatTap(self.index(project, _, m))
            }
            concat(
              // Link a file without an id segment
              (onCreationDirective & post) { (storage, tag, mode, request) =>
                emit(
                  Created,
                  files.linkFile(None, project, storage, request, tag).index(mode)
                )
              },
              // Link a file with id segment
              (idSegment & onCreationDirective & put) { (id, storage, tag, mode, request) =>
                emit(
                  Created,
                  files.linkFile(Some(id), project, storage, request, tag).index(mode)
                )
              },
              // Update a linked file
              (put & idSegment & indexingMode & pathEndOrSingleSlash) { (id, mode) =>
                (revParam & storageParam & tagParam) { (rev, storage, tag) =>
                  entity(as[FileLinkRequest]) { request =>
                    val fileId = FileId(id, project)
                    emit(
                      files.updateLinkedFile(fileId, rev, storage, request, tag).index(mode)
                    )
                  }
                }
              }
            )
          }
        }

      }
    }
}
