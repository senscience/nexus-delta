package ai.senscience.nexus.delta.plugins.storage.storages.routes

import ai.senscience.nexus.delta.plugins.storage.storages.*
import ai.senscience.nexus.delta.plugins.storage.storages.StoragePluginExceptionHandler.handleStorageExceptions
import ai.senscience.nexus.delta.plugins.storage.storages.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.plugins.storage.storages.routes.StoragesRoutes.AccessType
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaSchemeDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.{OriginalSource, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment, IdSegmentRef}
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.Json
import org.apache.pekko.http.scaladsl.model.StatusCodes.Created
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.unmarshalling.{FromStringUnmarshaller, Unmarshaller}
import org.typelevel.otel4s.trace.Tracer

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
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  import schemeDirectives.*

  private def emitMetadata(statusCode: StatusCode, io: IO[StorageResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[StorageResource]): Route = emitMetadata(StatusCodes.OK, io)

  def routes: Route =
    concat(storageRoutes, storagePermissionRoutes)

  private def storageRoutes =
    (baseUriPrefix(baseUri.prefix) & replaceUri("storages", schemas.storage)) {
      (handleStorageExceptions & pathPrefix("storages")) {
        extractCaller { implicit caller =>
          projectRef { project =>
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              // List storages
              pathEndOrSingleSlash {
                (get & authorizeRead) {
                  implicit val searchJsonLdEncoder: JsonLdEncoder[SearchResults[StorageResource]] =
                    searchResultsJsonLdEncoder(ContextValue(contexts.storages))
                  emit(storages.list(project))
                }
              },
              pathEndOrSingleSlash {
                // Create a storage without id segment
                (post & noParameter("rev") & entity(as[Json])) { source =>
                  authorizeWrite {
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
                        authorizeWrite {
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
                        authorizeWrite {
                          emitMetadata(storages.deprecate(id, project, rev))
                        }
                      },
                      // Fetch a storage
                      (get & idSegmentRef(id)) { id =>
                        emitOrFusionRedirect(
                          project,
                          id,
                          authorizeRead {
                            emit(storages.fetch(id, project))
                          }
                        )
                      }
                    )
                  },
                  // Undeprecate a storage
                  (pathPrefix("undeprecate") & pathEndOrSingleSlash & put & parameter("rev".as[Int])) { rev =>
                    authorizeWrite {
                      emitMetadata(storages.undeprecate(id, project, rev))
                    }
                  },
                  // Fetch a storage original source
                  (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(id)) { id =>
                    authorizeRead {
                      val sourceIO = storages
                        .fetch(id, project)
                        .map { resource => OriginalSource(resource, resource.value.source) }
                      emit(sourceIO)
                    }
                  },
                  (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
                    authorizeRead {
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

  private def storagePermissionRoutes =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("user") {
        pathPrefix("permissions") {
          projectRef { project =>
            extractCaller { implicit caller =>
              head {
                parameters("storage".as[IdSegment], "type".as[AccessType]) { (storageId, accessType) =>
                  fetchStoragePermission(project, storageId, accessType) { permission =>
                    authorizeFor(project, permission) {
                      complete(StatusCodes.NoContent)
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  private def fetchStoragePermission(project: ProjectRef, storageId: IdSegment, accessType: AccessType) = {
    val io = storages.fetch(IdSegmentRef(storageId), project).map { storage =>
      accessType match {
        case AccessType.Read  => storage.value.storageValue.readPermission
        case AccessType.Write => storage.value.storageValue.writePermission
      }
    }
    onSuccess(io.unsafeToFuture())
  }
}

object StoragesRoutes {

  enum AccessType {
    case Read
    case Write
  }

  object AccessType {
    given FromStringUnmarshaller[AccessType] =
      Unmarshaller.strict[String, AccessType] {
        case "read"  => AccessType.Read
        case "write" => AccessType.Write
        case string  =>
          throw new IllegalArgumentException(s"Access type can be either 'read' or 'write', received [$string]")
      }
  }

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
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO]): Route =
    new StoragesRoutes(identities, aclCheck, storages, storagesStatistics, schemeDirectives).routes

}
