package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.Vocabulary.schemas.shacl
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.routeSpan
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaSchemeDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.{OriginalSource, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.routes.Tag
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.schemas.{read as Read, write as Write}
import ai.senscience.nexus.delta.sdk.schemas.Schemas
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaRejection
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaRejection.SchemaNotFound
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import org.apache.pekko.http.scaladsl.model.StatusCodes.Created
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

/**
  * The schemas routes
  *
  * @param identities
  *   the identity module
  * @param aclCheck
  *   verify the acls for users
  * @param schemas
  *   the schemas module
  * @param schemeDirectives
  *   directives related to orgs and projects
  */
final class SchemasRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    schemas: Schemas,
    schemeDirectives: DeltaSchemeDirectives
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  import schemeDirectives.*

  private given [A: JsonLdEncoder]: JsonLdEncoder[ResourceF[A]] =
    ResourceF.resourceFAJsonLdEncoder(ContextValue(contexts.schemasMetadata))

  private def exceptionHandler(enableRejects: Boolean) =
    handleExceptions(
      ExceptionHandler { case err: SchemaRejection =>
        err match {
          case _: SchemaNotFound if enableRejects => reject(Reject(err))
          case _                                  => discardEntityAndForceEmit(err)
        }
      }
    )

  private def emitFetch(io: IO[SchemaResource]): Route =
    exceptionHandler(enableRejects = true) {
      emit(io)
    }

  private def emitMetadata(statusCode: StatusCode, io: IO[SchemaResource]): Route =
    exceptionHandler(enableRejects = false) {
      emit(statusCode, io.map(_.void))
    }

  private def emitMetadata(io: IO[SchemaResource]): Route = emitMetadata(StatusCodes.OK, io)

  private def emitMetadataOrReject(io: IO[SchemaResource]): Route =
    exceptionHandler(enableRejects = true) {
      emit(io.map(_.void))
    }

  private def emitSource(io: IO[SchemaResource], annotate: Boolean): Route =
    exceptionHandler(enableRejects = true) {
      emit(io.map { resource => OriginalSource(resource, resource.value.source, annotate) })
    }

  private def emitTags(io: IO[SchemaResource]): Route =
    exceptionHandler(enableRejects = true) {
      emit(io.map(_.value.tags))
    }

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & replaceUri("schemas", shacl)) {
      pathPrefix("schemas") {
        extractCaller { case given Caller =>
          projectRef { project =>
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              // List schemas
              routeSpan("schemas/<str:org>/<str:project>") {
                (pathEndOrSingleSlash & get & authorizeRead) {
                  given JsonLdEncoder[SearchResults[ResourceF[Unit]]] = searchResultsJsonLdEncoder(ContextValue.empty)
                  emit(schemas.list(project).map(_.map(_.void)).widen[SearchResults[ResourceF[Unit]]])
                }
              },
              // Create a schema without id segment
              routeSpan("schemas/<str:org>/<str:project>") {
                (pathEndOrSingleSlash & post & noRev & entity(as[Json])) { source =>
                  authorizeWrite {
                    emitMetadata(Created, schemas.create(project, source))
                  }
                }
              },
              idSegment { id =>
                concat(
                  (routeSpan("schemas/<str:org>/<str:project>/<str:id>") & pathEndOrSingleSlash) {
                    concat(
                      // Create or update a schema
                      put {
                        authorizeWrite {
                          (revParamOpt & entity(as[Json])) {
                            case (None, source)      =>
                              // Create a schema with id segment
                              emitMetadata(Created, schemas.create(id, project, source))
                            case (Some(rev), source) =>
                              // Update a schema
                              emitMetadata(schemas.update(id, project, rev, source))
                          }
                        }
                      },
                      // Deprecate a schema
                      (delete & revParam) { rev =>
                        authorizeWrite {
                          emitMetadataOrReject(schemas.deprecate(id, project, rev))
                        }
                      },
                      // Fetch a schema
                      (get & idSegmentRef(id)) { id =>
                        emitOrFusionRedirect(
                          project,
                          id,
                          authorizeRead {
                            emitFetch(schemas.fetch(id, project))
                          }
                        )
                      }
                    )
                  },
                  routeSpan("schemas/<str:org>/<str:project>/<str:id>/undeprecate") {
                    (pathPrefix("undeprecate") & put & pathEndOrSingleSlash & revParam) { rev =>
                      authorizeWrite {
                        emitMetadataOrReject(schemas.undeprecate(id, project, rev))
                      }
                    }
                  },
                  routeSpan("schemas/<str:org>/<str:project>/<str:id>/refresh") {
                    (pathPrefix("refresh") & put & pathEndOrSingleSlash) {
                      authorizeWrite {
                        emitMetadata(schemas.refresh(id, project))
                      }
                    }
                  },
                  // Fetch a schema original source
                  routeSpan("schemas/<str:org>/<str:project>/<str:id>/source") {
                    (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(id) & annotateSource) {
                      (id, annotate) =>
                        authorizeRead {
                          emitSource(schemas.fetch(id, project), annotate)
                        }
                    }
                  },
                  routeSpan("schemas/<str:org>/<str:project>/<str:id>/tags") {
                    pathPrefix("tags") {
                      concat(
                        // Fetch a schema tags
                        (get & idSegmentRef(id) & pathEndOrSingleSlash & authorizeRead) { id =>
                          emitTags(schemas.fetch(id, project))
                        },
                        // Tag a schema
                        (post & revParam & pathEndOrSingleSlash) { rev =>
                          authorizeWrite {
                            entity(as[Tag]) { case Tag(tagRev, tag) =>
                              emitMetadata(Created, schemas.tag(id, project, tag, tagRev, rev))
                            }
                          }
                        },
                        // Delete a tag
                        (tagLabel & delete & revParam & pathEndOrSingleSlash & authorizeWrite) { (tag, rev) =>
                          emitMetadataOrReject(schemas.deleteTag(id, project, tag, rev))
                        }
                      )
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

object SchemasRoutes {

  /**
    * @return
    *   the [[Route]] for schemas
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      schemas: Schemas,
      schemeDirectives: DeltaSchemeDirectives
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO]): Route =
    new SchemasRoutes(identities, aclCheck, schemas, schemeDirectives).routes

}
