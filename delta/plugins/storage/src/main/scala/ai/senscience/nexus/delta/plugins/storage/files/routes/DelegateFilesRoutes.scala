package ai.senscience.nexus.delta.plugins.storage.files.routes

import ai.senscience.nexus.delta.plugins.storage.files.model.*
import ai.senscience.nexus.delta.plugins.storage.files.model.FileDelegationRequest.{FileDelegationCreationRequest, FileDelegationUpdateRequest}
import ai.senscience.nexus.delta.plugins.storage.files.routes.FileUriDirectives.storageParam
import ai.senscience.nexus.delta.plugins.storage.files.{FileResource, Files}
import ai.senscience.nexus.delta.plugins.storage.storages.StoragePluginExceptionHandler.handleStorageExceptions
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.ShowFileLocation
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, ResponseToJsonLd}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.indexing.{IndexingAction, IndexingMode}
import ai.senscience.nexus.delta.sdk.jws.JWSPayloadHelper
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.pekko.marshalling.{CirceMarshalling, CirceUnmarshalling}
import cats.effect.IO
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.pekko.http.scaladsl.model.StatusCodes.{Created, OK}
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

final class DelegateFilesRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    files: Files,
    jwsPayloadHelper: JWSPayloadHelper,
    index: IndexingAction.Execute[File]
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, ShowFileLocation, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with CirceMarshalling { self =>

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      (pathPrefix("delegate" / "files") & handleStorageExceptions) {
        extractCaller { implicit caller =>
          concat(
            (pathPrefix("generate") & projectRef) { project =>
              concat(
                // Delegate a file creation without id segment
                (pathEndOrSingleSlash & post & storageParam & tagParam & noRev) { case (storageId, tag) =>
                  entity(as[FileDescription]) { desc =>
                    emit(OK, createDelegation(None, project, storageId, desc, tag))
                  }
                },
                // Delegate a file creation without id segment
                (idSegment & pathEndOrSingleSlash & put & storageParam & noRev & tagParam) { (id, storageId, tag) =>
                  entity(as[FileDescription]) { desc =>
                    emit(OK, createDelegation(Some(id), project, storageId, desc, tag))
                  }
                },
                // Delegate a file creation without id segment
                (idSegment & pathEndOrSingleSlash & put & storageParam & revParam & tagParam) {
                  (id, storageId, rev, tag) =>
                    entity(as[FileDescription]) { desc =>
                      emit(OK, updateDelegation(id, project, rev, storageId, desc, tag))
                    }
                }
              )
            },
            (pathPrefix("submit") & put & pathEndOrSingleSlash & entity(as[Json]) & indexingMode) {
              (jwsPayload, mode) =>
                emit(
                  Created,
                  linkDelegatedFile(jwsPayload, mode): ResponseToJsonLd
                )
            }
          )
        }
      }
    }

  private def createDelegation(
      id: Option[IdSegment],
      project: ProjectRef,
      storageId: Option[IdSegment],
      desc: FileDescription,
      tag: Option[UserTag]
  )(using Caller) =
    files.createDelegate(id, project, desc, storageId, tag).flatMap { request =>
      jwsPayloadHelper.sign(request.asJson)
    }

  private def updateDelegation(
      id: IdSegment,
      project: ProjectRef,
      rev: Int,
      storageId: Option[IdSegment],
      desc: FileDescription,
      tag: Option[UserTag]
  )(using Caller) =
    files.updateDelegate(id, project, rev, desc, storageId, tag).flatMap { request =>
      jwsPayloadHelper.sign(request.asJson)
    }

  private def linkDelegatedFile(
      jwsPayload: Json,
      mode: IndexingMode
  )(using Caller): IO[FileResource] =
    jwsPayloadHelper
      .verifyAs[FileDelegationRequest](jwsPayload)
      .flatMap {
        case FileDelegationCreationRequest(project, id, targetLocation, description, tag)    =>
          val linkRequest = FileLinkRequest(targetLocation.path.path, description.mediaType, description.metadata)
          files.linkFile(Some(id), project, Some(targetLocation.storageId), linkRequest, tag)
        case FileDelegationUpdateRequest(project, id, rev, targetLocation, description, tag) =>
          val fileId      = FileId(id, project)
          val linkRequest = FileLinkRequest(targetLocation.path.path, description.mediaType, description.metadata)
          files.updateLinkedFile(fileId, rev, Some(targetLocation.storageId), linkRequest, tag)
      }
      .flatTap { fileResource =>
        index(fileResource.value.project, fileResource, mode)
      }
}
