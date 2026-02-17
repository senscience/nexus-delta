package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.Vocabulary.schemas
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.*
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.indexing.{IndexingMode, SyncIndexingAction}
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.routes.Tag
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources.{delete as Delete, read as Read, write as Write}
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.*
import ai.senscience.nexus.delta.sdk.resources.model.{Resource, ResourceRejection}
import ai.senscience.nexus.delta.sdk.resources.{NexusSource, Resources}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.syntax.all.*
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.StatusCodes.Created
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

/**
  * The resource routes
  *
  * @param identities
  *   the identity module
  * @param aclCheck
  *   verify the acls for users
  * @param resources
  *   the resources module
  * @param index
  *   the indexing action on write operations
  */
final class ResourcesRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    resources: Resources,
    index: SyncIndexingAction.Execute[Resource]
)(using baseUri: BaseUri)(using
    RemoteContextResolution,
    JsonKeyOrdering,
    FusionConfig,
    Tracer[IO]
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling { self =>

  private val resourceSchema = schemas.resources

  private given [A: JsonLdEncoder] => JsonLdEncoder[ResourceF[A]] =
    ResourceF.resourceFAJsonLdEncoder(ContextValue.empty)

  private val nonGenericResourceCandidate: Throwable => Boolean = {
    case _: ResourceNotFound | _: InvalidSchemaRejection | _: ReservedResourceTypes => true
    case _                                                                          => false
  }

  private val notFound: Throwable => Boolean = {
    case _: ResourceNotFound => true
    case _                   => false
  }

  private def exceptionHandler(filterRejection: Throwable => Boolean) =
    handleExceptions(
      ExceptionHandler {
        case r: ResourceRejection if filterRejection(r) => reject(Reject(r))
        case r: ResourceRejection                       => discardEntityAndForceEmit(r)
      }
    )

  private val rejectOnNonGeneric = exceptionHandler(nonGenericResourceCandidate)
  private val rejectOnNotFound   = exceptionHandler(notFound)

  private val resourceEntity = entity(as[NexusSource])

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("resources") {
        extractCaller { case caller @ given Caller =>
          given Subject = caller.subject
          projectRef { project =>
            val authorizeDelete = authorizeFor(project, Delete)
            val authorizeRead   = authorizeFor(project, Read)
            val authorizeWrite  = authorizeFor(project, Write)
            extension (io: IO[DataResource]) {
              private def index(m: IndexingMode): IO[ResourceF[Unit]] =
                io.flatTap(self.index(project, _, m)).map(_.void)
            }
            concat(
              // Create a resource without schema nor id segment
              (pathEndOrSingleSlash & post & noRev & resourceEntity & indexingMode & tagParam) { (source, mode, tag) =>
                routeSpan("resources/<str:org>/<str:project>") {
                  (authorizeWrite & rejectOnNonGeneric) {
                    emit(
                      Created,
                      resources.create(project, resourceSchema, source.value, tag).index(mode)
                    )
                  }
                }
              },
              (idSegmentRef & indexingMode) { (schema, mode) =>
                val schemaOpt = underscoreToOption(schema)
                concat(
                  // Create a resource with schema but without id segment
                  (pathEndOrSingleSlash & post & noRev & resourceEntity & tagParam) { (source, tag) =>
                    routeSpan("resources/<str:org>/<str:project>/<str:schema>") {
                      (authorizeWrite & rejectOnNonGeneric) {
                        emit(
                          Created,
                          resources.create(project, schema, source.value, tag).index(mode)
                        )
                      }
                    }
                  },
                  idSegment { resource =>
                    concat(
                      pathEndOrSingleSlash {
                        routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>") {
                          concat(
                            // Create or update a resource
                            put {
                              (authorizeWrite & rejectOnNonGeneric) {
                                (revParamOpt & pathEndOrSingleSlash & resourceEntity & tagParam) {
                                  case (None, source, tag)      =>
                                    // Create a resource with schema and id segments
                                    emit(
                                      Created,
                                      resources.create(resource, project, schema, source.value, tag).index(mode)
                                    )
                                  case (Some(rev), source, tag) =>
                                    // Update a resource
                                    emit(
                                      resources.update(resource, project, schemaOpt, rev, source.value, tag).index(mode)
                                    )
                                }
                              }
                            },
                            // Deprecate a resource
                            (pathEndOrSingleSlash & delete) {
                              concat(
                                revParam { rev =>
                                  (authorizeWrite & rejectOnNotFound) {
                                    emit(
                                      resources.deprecate(resource, project, schemaOpt, rev).index(mode).map(_.void)
                                    )
                                  }
                                },
                                (prune & authorizeDelete) {
                                  emit(StatusCodes.NoContent, resources.delete(resource, project))
                                }
                              )
                            },
                            // Fetch a resource
                            (pathEndOrSingleSlash & get & idSegmentRef(resource) & varyAcceptHeaders) { resourceRef =>
                              emitOrFusionRedirect(
                                project,
                                resourceRef,
                                (authorizeRead & rejectOnNotFound) {
                                  emit(resources.fetch(resourceRef, project, schemaOpt))
                                }
                              )
                            }
                          )
                        }
                      },
                      // Undeprecate a resource
                      (pathPrefix("undeprecate") & put & revParam) { rev =>
                        routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>/undeprecate") {
                          (authorizeWrite & rejectOnNotFound) {
                            emit(
                              resources.undeprecate(resource, project, schemaOpt, rev).index(mode)
                            )
                          }
                        }
                      },
                      (pathPrefix("update-schema") & put & pathEndOrSingleSlash) {
                        routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>/update-schema") {
                          (authorizeWrite & rejectOnNotFound) {
                            emit(
                              IO.fromOption(schemaOpt)(NoSchemaProvided)
                                .flatMap { schema =>
                                  resources.updateAttachedSchema(resource, project, schema).index(mode)
                                }
                            )
                          }
                        }
                      },
                      (pathPrefix("refresh") & put & pathEndOrSingleSlash) {
                        routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>/refresh") {
                          (authorizeWrite & rejectOnNotFound) {
                            emit(
                              resources.refresh(resource, project, schemaOpt).index(mode)
                            )
                          }
                        }
                      },
                      // Fetch a resource original source
                      (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(resource) & varyAcceptHeaders) {
                        resourceRef =>
                          routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>/source") {
                            (authorizeRead & rejectOnNotFound) {
                              annotateSource { annotate =>
                                emit(
                                  resources
                                    .fetch(resourceRef, project, schemaOpt)
                                    .map { resource => OriginalSource(resource, resource.value.source, annotate) }
                                )
                              }
                            }
                          }
                      },
                      // Get remote contexts
                      pathPrefix("remote-contexts") {
                        (get & idSegmentRef(resource) & pathEndOrSingleSlash & authorizeRead & exceptionHandler(_ =>
                          false
                        )) { resourceRef =>
                          routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>/remote-contexts") {
                            emit(resources.fetchState(resourceRef, project, schemaOpt).map(_.remoteContexts))
                          }
                        }
                      },
                      // Tag a resource
                      pathPrefix("tags") {
                        routeSpan("resources/<str:org>/<str:project>/<str:schema>/<str:id>/tags") {
                          concat(
                            // Fetch a resource tags
                            (get & idSegmentRef(resource) & pathEndOrSingleSlash & authorizeRead & rejectOnNotFound) {
                              resourceRef =>
                                emit(
                                  resources.fetch(resourceRef, project, schemaOpt).map(_.value.tags)
                                )
                            },
                            // Tag a resource
                            (post & revParam & pathEndOrSingleSlash) { rev =>
                              (authorizeWrite & exceptionHandler(notFound) & entity(as[Tag])) { case Tag(tagRev, tag) =>
                                emit(
                                  Created,
                                  resources
                                    .tag(resource, project, schemaOpt, tag, tagRev, rev)
                                    .index(mode)
                                )
                              }
                            },
                            // Delete a tag
                            (tagLabel & delete & revParam & pathEndOrSingleSlash & authorizeWrite & rejectOnNotFound) {
                              (tag, rev) =>
                                emit(resources.deleteTag(resource, project, schemaOpt, tag, rev).index(mode))
                            }
                          )
                        }
                      }
                    )
                  }
                )
              }
            )
          }
        }
      }
    }
}

object ResourcesRoutes {

  /**
    * @return
    *   the [[Route]] for resources
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      resources: Resources,
      index: SyncIndexingAction.Execute[Resource]
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO]): Route =
    new ResourcesRoutes(identities, aclCheck, resources, index).routes

}
