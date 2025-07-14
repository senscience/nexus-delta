package ai.senscience.nexus.delta.plugins.storage.storages.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.plugins.storage.storages.StoragePluginExceptionHandler.handleStorageExceptions
import ai.senscience.nexus.delta.plugins.storage.storages.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.storage.storages.{StorageResource, Storages, StoragesStatistics, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaSchemeDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.{OriginalSource, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.*
import cats.effect.IO
import io.circe.Json

/**
  * The storages routes
  *
  * @param identities
  *   the identity module
  * @param aclCheck
  *   how to check acls
  * @param storages
  *   the storages module
  * @param schemeDirectives
  *   directives related to orgs and projects
  */
final class StoragesRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    storages: Storages,
    storagesStatistics: StoragesStatistics,
    schemeDirectives: DeltaSchemeDirectives
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering,
    fusionConfig: FusionConfig
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  import schemeDirectives.*

  private def emitMetadata(statusCode: StatusCode, io: IO[StorageResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[StorageResource]): Route = emitMetadata(StatusCodes.OK, io)

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & replaceUri("storages", schemas.storage)) {
      (handleStorageExceptions & pathPrefix("storages")) {
        extractCaller { implicit caller =>
          projectRef { project =>
            concat(
              pathEndOrSingleSlash {
                // Create a storage without id segment
                (post & noParameter("rev") & entity(as[Json])) { source =>
                  authorizeFor(project, Write).apply {
                    emitMetadata(Created, storages.create(project, source))
                  }
                }
              },
              idSegment { id =>
                concat(
                  pathEndOrSingleSlash {
                    concat(
                      // Create or update a storage
                      put {
                        authorizeFor(project, Write).apply {
                          (parameter("rev".as[Int].?) & pathEndOrSingleSlash & entity(as[Json])) {
                            case (None, source)      =>
                              // Create a storage with id segment
                              emitMetadata(Created, storages.create(id, project, source))
                            case (Some(rev), source) =>
                              // Update a storage
                              emitMetadata(storages.update(id, project, rev, source))
                          }
                        }
                      },
                      // Deprecate a storage
                      (delete & parameter("rev".as[Int])) { rev =>
                        authorizeFor(project, Write).apply {
                          emitMetadata(storages.deprecate(id, project, rev))
                        }
                      },
                      // Fetch a storage
                      (get & idSegmentRef(id)) { id =>
                        emitOrFusionRedirect(
                          project,
                          id,
                          authorizeFor(project, Read).apply {
                            emit(storages.fetch(id, project))
                          }
                        )
                      }
                    )
                  },
                  // Undeprecate a storage
                  (pathPrefix("undeprecate") & pathEndOrSingleSlash & put & parameter("rev".as[Int])) { rev =>
                    authorizeFor(project, Write).apply {
                      emitMetadata(storages.undeprecate(id, project, rev))
                    }
                  },
                  // Fetch a storage original source
                  (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(id)) { id =>
                    authorizeFor(project, Read).apply {
                      val sourceIO = storages
                        .fetch(id, project)
                        .map { resource => OriginalSource(resource, resource.value.source) }
                      emit(sourceIO)
                    }
                  },
                  (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
                    authorizeFor(project, Read).apply {
                      emit(storagesStatistics.get(id, project))
                    }
                  }
                )
              }
            )
          }
        }
      }
    }
}

object StoragesRoutes {

  /**
    * @return
    *   the [[Route]] for storages
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      storages: Storages,
      storagesStatistics: StoragesStatistics,
      schemeDirectives: DeltaSchemeDirectives
  )(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering,
      fusionConfig: FusionConfig
  ): Route =
    new StoragesRoutes(identities, aclCheck, storages, storagesStatistics, schemeDirectives).routes

}
