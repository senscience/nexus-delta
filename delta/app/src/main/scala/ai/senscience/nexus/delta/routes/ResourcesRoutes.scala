package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.rdf.Vocabulary.schemas
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.indexing.{IndexingAction, IndexingMode}
import ai.senscience.nexus.delta.sdk.marshalling.{OriginalSource, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.routes.Tag
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources.{delete as Delete, read as Read, write as Write}
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.*
import ai.senscience.nexus.delta.sdk.resources.model.{Resource, ResourceRejection}
import ai.senscience.nexus.delta.sdk.resources.{NexusSource, Resources}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.server.*
import cats.effect.IO
import cats.syntax.all.*

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
    index: IndexingAction.Execute[Resource]
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering,
    fusionConfig: FusionConfig
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling { self =>

  private val resourceSchema = schemas.resources

  implicit private def resourceFAJsonLdEncoder[A: JsonLdEncoder]: JsonLdEncoder[ResourceF[A]] =
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

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("resources") {
        extractCaller { implicit caller =>
          projectRef { project =>
            val authorizeDelete = authorizeFor(project, Delete)
            val authorizeRead   = authorizeFor(project, Read)
            val authorizeWrite  = authorizeFor(project, Write)
            implicit class IndexOps(io: IO[DataResource]) {
              def index(m: IndexingMode): IO[DataResource] = io.flatTap(self.index(project, _, m))
            }
            concat(
              // Create a resource without schema nor id segment
              (pathEndOrSingleSlash & post & noRev & entity(as[NexusSource]) & indexingMode & tagParam) {
                (source, mode, tag) =>
                  (authorizeWrite & exceptionHandler(nonGenericResourceCandidate)) {
                    emit(
                      Created,
                      resources
                        .create(project, resourceSchema, source.value, tag)
                        .index(mode)
                        .map(_.void)
                    )
                  }
              },
              (idSegment & indexingMode) { (schema, mode) =>
                val schemaOpt = underscoreToOption(schema)
                concat(
                  // Create a resource with schema but without id segment
                  (pathEndOrSingleSlash & post & noRev & entity(as[NexusSource]) & tagParam) { (source, tag) =>
                    (authorizeWrite & exceptionHandler(nonGenericResourceCandidate)) {
                      emit(
                        Created,
                        resources
                          .create(project, schema, source.value, tag)
                          .index(mode)
                          .map(_.void)
                      )
                    }
                  },
                  idSegment { resource =>
                    concat(
                      pathEndOrSingleSlash {
                        concat(
                          // Create or update a resource
                          put {
                            (authorizeWrite & exceptionHandler(nonGenericResourceCandidate)) {
                              (parameter("rev".as[Int].?) & pathEndOrSingleSlash & entity(as[NexusSource]) & tagParam) {
                                case (None, source, tag)      =>
                                  // Create a resource with schema and id segments
                                  emit(
                                    Created,
                                    resources
                                      .create(resource, project, schema, source.value, tag)
                                      .index(mode)
                                      .map(_.void)
                                  )
                                case (Some(rev), source, tag) =>
                                  // Update a resource
                                  emit(
                                    resources
                                      .update(resource, project, schemaOpt, rev, source.value, tag)
                                      .index(mode)
                                      .map(_.void)
                                  )
                              }
                            }
                          },
                          // Deprecate a resource
                          (pathEndOrSingleSlash & delete) {
                            concat(
                              parameter("rev".as[Int]) { rev =>
                                (authorizeWrite & exceptionHandler(notFound)) {
                                  emit(
                                    resources
                                      .deprecate(resource, project, schemaOpt, rev)
                                      .index(mode)
                                      .map(_.void)
                                  )
                                }
                              },
                              (prune & authorizeDelete) {
                                emit(
                                  StatusCodes.NoContent,
                                  resources.delete(resource, project)
                                )
                              }
                            )
                          },
                          // Fetch a resource
                          (pathEndOrSingleSlash & get & idSegmentRef(resource) & varyAcceptHeaders) { resourceRef =>
                            emitOrFusionRedirect(
                              project,
                              resourceRef,
                              (authorizeRead & exceptionHandler(notFound)) {
                                emit(resources.fetch(resourceRef, project, schemaOpt))
                              }
                            )
                          }
                        )
                      },
                      // Undeprecate a resource
                      (pathPrefix("undeprecate") & put & parameter("rev".as[Int])) { rev =>
                        (authorizeWrite & exceptionHandler(notFound)) {
                          emit(
                            resources
                              .undeprecate(resource, project, schemaOpt, rev)
                              .index(mode)
                              .map(_.void)
                          )
                        }
                      },
                      (pathPrefix("update-schema") & put & pathEndOrSingleSlash) {
                        (authorizeWrite & exceptionHandler(notFound)) {
                          emit(
                            IO.fromOption(schemaOpt)(NoSchemaProvided)
                              .flatMap { schema =>
                                resources
                                  .updateAttachedSchema(resource, project, schema)
                                  .flatTap(index(project, _, mode))
                              }
                          )
                        }
                      },
                      (pathPrefix("refresh") & put & pathEndOrSingleSlash) {
                        (authorizeWrite & exceptionHandler(notFound)) {
                          emit(
                            resources
                              .refresh(resource, project, schemaOpt)
                              .index(mode)
                              .map(_.void)
                          )
                        }
                      },
                      // Fetch a resource original source
                      (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(resource) & varyAcceptHeaders) {
                        resourceRef =>
                          (authorizeRead & exceptionHandler(notFound)) {
                            annotateSource { annotate =>
                              emit(
                                resources
                                  .fetch(resourceRef, project, schemaOpt)
                                  .map { resource => OriginalSource(resource, resource.value.source, annotate) }
                              )
                            }
                          }
                      },
                      // Get remote contexts
                      pathPrefix("remote-contexts") {
                        (get & idSegmentRef(resource) & pathEndOrSingleSlash & authorizeRead & exceptionHandler(_ =>
                          false
                        )) { resourceRef =>
                          emit(
                            resources
                              .fetchState(resourceRef, project, schemaOpt)
                              .map(_.remoteContexts)
                          )
                        }
                      },
                      // Tag a resource
                      pathPrefix("tags") {
                        concat(
                          // Fetch a resource tags
                          (get & idSegmentRef(resource) & pathEndOrSingleSlash & authorizeRead & exceptionHandler(
                            notFound
                          )) { resourceRef =>
                            emit(
                              resources
                                .fetch(resourceRef, project, schemaOpt)
                                .map(_.value.tags)
                            )
                          },
                          // Tag a resource
                          (post & parameter("rev".as[Int]) & pathEndOrSingleSlash) { rev =>
                            (authorizeWrite & exceptionHandler(notFound)) {
                              entity(as[Tag]) { case Tag(tagRev, tag) =>
                                emit(
                                  Created,
                                  resources
                                    .tag(resource, project, schemaOpt, tag, tagRev, rev)
                                    .index(mode)
                                    .map(_.void)
                                )
                              }
                            }
                          },
                          // Delete a tag
                          (tagLabel & delete & parameter(
                            "rev".as[Int]
                          ) & pathEndOrSingleSlash & authorizeWrite & exceptionHandler(notFound)) { (tag, rev) =>
                            emit(
                              resources
                                .deleteTag(resource, project, schemaOpt, tag, rev)
                                .index(mode)
                                .map(_.void)
                            )
                          }
                        )
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
      index: IndexingAction.Execute[Resource]
  )(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering,
      fusionConfig: FusionConfig
  ): Route = new ResourcesRoutes(identities, aclCheck, resources, index).routes

}
