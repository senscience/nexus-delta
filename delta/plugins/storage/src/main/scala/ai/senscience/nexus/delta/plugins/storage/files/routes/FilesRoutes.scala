package ai.senscience.nexus.delta.plugins.storage.files.routes

import ai.senscience.nexus.delta.plugins.storage.files.model.*
import ai.senscience.nexus.delta.plugins.storage.files.model.FileRejection.*
import ai.senscience.nexus.delta.plugins.storage.files.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.storage.files.routes.FileUriDirectives.*
import ai.senscience.nexus.delta.plugins.storage.files.routes.FilesRoutes.*
import ai.senscience.nexus.delta.plugins.storage.files.{schemas, FileResource, Files}
import ai.senscience.nexus.delta.plugins.storage.storages.StoragePluginExceptionHandler.handleStorageExceptions
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.ShowFileLocation
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaSchemeDirectives}
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.given
import ai.senscience.nexus.delta.sdk.indexing.{IndexingMode, SyncIndexingAction}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.routes.Tag
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import io.circe.parser
import org.apache.pekko.http.scaladsl.model.MediaRange
import org.apache.pekko.http.scaladsl.model.StatusCodes.Created
import org.apache.pekko.http.scaladsl.model.headers.{`Content-Length`, Accept}
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.server.Directives.{extractRequestEntity, optionalHeaderValueByName, provide, reject}
import org.typelevel.otel4s.trace.Tracer

/**
  * The files routes
  *
  * @param identities
  *   the identity module
  * @param aclCheck
  *   to check acls
  * @param files
  *   the files module
  * @param schemeDirectives
  *   directives related to orgs and projects
  * @param index
  *   the indexing action on write operations
  */
final class FilesRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    files: Files,
    schemeDirectives: DeltaSchemeDirectives,
    index: SyncIndexingAction.Execute[File]
)(using baseUri: BaseUri)(using ShowFileLocation, RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling { self =>

  import schemeDirectives.*

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & replaceUri("files", schemas.files)) {
      (handleStorageExceptions & pathPrefix("files")) {
        extractCaller { case caller @ given Caller =>
          given Subject = caller.subject
          projectRef { project =>
            extension (io: IO[FileResource]) {
              private def index(m: IndexingMode): IO[FileResource] = io.flatTap(self.index(project, _, m))
            }
            concat(
              (pathEndOrSingleSlash & post & noRev & storageParam & indexingMode & tagParam) { (storage, mode, tag) =>
                concat(
                  // Create a file without id segment
                  uploadRequest { request =>
                    emit(Created, files.create(storage, project, request, tag).index(mode))
                  }
                )
              },
              (idSegment & indexingMode) { (id, mode) =>
                val fileId = FileId(id, project)
                concat(
                  pathEndOrSingleSlash {
                    concat(
                      (put & pathEndOrSingleSlash) {
                        concat(
                          (revParam & storageParam & tagParam) { case (rev, storage, tag) =>
                            concat(
                              // Update a file
                              (requestEntityPresent & uploadRequest) { request =>
                                emit(files.update(fileId, storage, rev, request, tag).index(mode))
                              },
                              // Update custom metadata
                              (requestEntityEmpty & extractFileMetadata & authorizeFor(project, Write)) {
                                case Some(FileCustomMetadata.empty) =>
                                  emit(IO.raiseError[FileResource](EmptyCustomMetadata))
                                case Some(metadata)                 =>
                                  emit(files.updateMetadata(fileId, rev, metadata, tag).index(mode))
                                case None                           => reject
                              }
                            )
                          },
                          (storageParam & tagParam) { case (storage, tag) =>
                            concat(
                              // Create a file with id segment
                              uploadRequest { request =>
                                emit(Created, files.create(fileId, storage, request, tag).index(mode))
                              }
                            )
                          }
                        )
                      },
                      // Deprecate a file
                      (delete & revParam) { rev =>
                        authorizeFor(project, Write).apply {
                          emit(files.deprecate(fileId, rev).index(mode))
                        }
                      },

                      // Fetch a file
                      (get & idSegmentRef(id)) { id =>
                        emitOrFusionRedirect(project, id, fetch(FileId(id, project)))
                      }
                    )
                  },
                  pathPrefix("tags") {
                    concat(
                      // Fetch a file tags
                      (get & idSegmentRef(id) & pathEndOrSingleSlash & authorizeFor(project, Read)) { id =>
                        emit(fetchMetadata(FileId(id, project)).map(_.value.tags))
                      },
                      // Tag a file
                      (post & revParam & pathEndOrSingleSlash) { rev =>
                        authorizeFor(project, Write).apply {
                          entity(as[Tag]) { case Tag(tagRev, tag) =>
                            emit(Created, files.tag(fileId, tag, tagRev, rev).index(mode))
                          }
                        }
                      },
                      // Delete a tag
                      (tagLabel & delete & revParam & pathEndOrSingleSlash & authorizeFor(
                        project,
                        Write
                      )) { (tag, rev) =>
                        emit(files.deleteTag(fileId, tag, rev).index(mode))
                      }
                    )
                  },
                  (pathPrefix("undeprecate") & put & revParam) { rev =>
                    authorizeFor(project, Write).apply {
                      emit(files.undeprecate(fileId, rev).index(mode))
                    }
                  }
                )
              }
            )
          }
        }
      }
    }

  def fetch(id: FileId)(using Caller): Route =
    (headerValueByType(Accept) & varyAcceptHeaders) {
      case accept if accept.mediaRanges.exists(metadataMediaRanges.contains) =>
        emit(fetchMetadata(id))
      case _                                                                 =>
        emit(files.fetchContent(id))
    }

  def fetchMetadata(id: FileId)(using Caller): IO[FileResource] =
    aclCheck.authorizeForOr(id.project, Read)(AuthorizationFailed(id.project, Read)) >> files.fetch(id)
}

object FilesRoutes {

  // If accept header media range exactly match one of these, we return file metadata,
  // otherwise we return the file content
  val metadataMediaRanges: Set[MediaRange] = mediaTypes.map(_.toContentType.mediaType: MediaRange).toSet

  /**
    * @return
    *   the [[Route]] for files
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      files: Files,
      schemeDirectives: DeltaSchemeDirectives,
      index: SyncIndexingAction.Execute[File]
  )(using BaseUri, ShowFileLocation, RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO]): Route =
    new FilesRoutes(identities, aclCheck, files, schemeDirectives, index).routes

  /**
    * A pekko directive to extract the optional [[FileCustomMetadata]] from a request. This metadata is extracted from
    * the `x-nxs-file-metadata` header. In case the decoding fails, a [[MalformedHeaderRejection]] is returned.
    */
  def extractFileMetadata: Directive1[Option[FileCustomMetadata]] =
    optionalHeaderValueByName(NexusHeaders.fileMetadata).flatMap {
      case Some(metadata) =>
        val md = parser.parse(metadata).flatMap(_.as[FileCustomMetadata])
        md match {
          case Right(value) => provide(Some(value))
          case Left(err)    => reject(MalformedHeaderRejection(NexusHeaders.fileMetadata, err.getMessage))
        }
      case None           => provide(Some(FileCustomMetadata.empty))
    }

  def fileContentLength: Directive1[Option[Long]] = {
    optionalHeaderValueByName(NexusHeaders.fileContentLength).flatMap {
      case Some(value) =>
        value.toLongOption match {
          case None =>
            val msg =
              s"Invalid '${NexusHeaders.fileContentLength}' header value '$value', expected a Long value."
            reject(MalformedHeaderRejection(`Content-Length`.name, msg))
          case v    => provide(v)
        }
      case None        => provide(None)
    }
  }

  def uploadRequest: Directive1[FileUploadRequest] =
    (extractRequestEntity & extractFileMetadata & fileContentLength).tflatMap {
      case (entity, customMetadata, contentLength) =>
        provide(FileUploadRequest(entity, customMetadata, contentLength))
    }
}
