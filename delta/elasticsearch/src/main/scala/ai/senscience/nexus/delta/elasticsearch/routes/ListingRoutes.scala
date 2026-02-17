package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.model.contexts
import ai.senscience.nexus.delta.elasticsearch.query.{MainIndexQuery, MainIndexRequest}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.routeSpan
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaSchemeDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources
import ai.senscience.nexus.delta.sdk.projects.ProjectScopeResolver
import ai.senscience.nexus.delta.sdk.projects.model.ProjectContext
import ai.senscience.nexus.delta.sdk.resources.Resources
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.{Label, ResourceRef}
import cats.effect.unsafe.implicits.*
import cats.effect.IO
import io.circe.JsonObject
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

class ListingRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    projectScopeResolver: ProjectScopeResolver,
    resourcesToSchemas: ResourceToSchemaMappings,
    schemeDirectives: DeltaSchemeDirectives,
    mainIndexQuery: MainIndexQuery
)(using BaseUri, PaginationConfig, RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with ElasticSearchViewsDirectives
    with RdfMarshalling {

  import schemeDirectives.*

  private def resourceRef(idSegment: IdSegmentRef)(using pc: ProjectContext): Directive1[ResourceRef] =
    onSuccess(Resources.expandIri(idSegment, pc).unsafeToFuture())

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      concat(genericResourcesRoutes, resourcesListings)
    }

  private val genericResourcesRoutes: Route =
    pathPrefix("resources") {
      extractCaller { case given Caller =>
        concat(
          (searchParametersAndSortList(None) & paginated) { (params, sort, page) =>
            val request = MainIndexRequest(params, page, sort)
            concat(
              // List/aggregate all resources
              (routeSpan("resources") & pathEndOrSingleSlash) {
                concat(
                  aggregate(request, Scope.Root),
                  list(request, Scope.Root)
                )
              },
              (routeSpan("resources/<str:org>") & label & pathEndOrSingleSlash) { org =>
                val scope = Scope.Org(org)
                concat(
                  aggregate(request, scope),
                  list(request, scope)
                )
              }
            )
          },
          (routeSpan("resources/<str:org>/<str:project>") & projectRef) { project =>
            projectContext(project) { case pc @ given ProjectContext =>
              (get & searchParametersAndSortList(Some(pc)) & paginated) { (params, sort, page) =>
                val scope = Scope.Project(project)
                concat(
                  // List/aggregate all resources inside a project
                  pathEndOrSingleSlash {
                    val request = MainIndexRequest(params, page, sort)
                    concat(
                      aggregate(request, scope),
                      list(request, scope)
                    )
                  },
                  idSegment { schema =>
                    // List/aggregate all resources inside a project filtering by its schema type
                    pathEndOrSingleSlash {
                      underscoreToOption(schema) match {
                        case None                =>
                          val request = MainIndexRequest(params, page, sort)
                          concat(
                            aggregate(request, scope),
                            list(request, scope)
                          )
                        case Some(schemaSegment) =>
                          resourceRef(schemaSegment).apply { schemaRef =>
                            val request =
                              MainIndexRequest(params.withSchema(schemaRef), page, sort)
                            concat(
                              aggregate(request, scope),
                              list(request, scope)
                            )
                          }
                      }
                    }
                  }
                )
              }
            }
          }
        )
      }
    }

  private def mainIndexRequest(pc: Option[ProjectContext], resourceSchema: Iri) =
    (searchParametersAndSortList(pc) & paginated).tmap { (params, sort, page) =>
      MainIndexRequest(params.withSchema(resourceSchema), page, sort)
    }

  private val resourcesListings: Route =
    concat(resourcesToSchemas.value.map { case (Label(resourceSegment), resourceSchema) =>
      pathPrefix(resourceSegment) {
        extractCaller { case given Caller =>
          concat(
            mainIndexRequest(None, resourceSchema) { request =>
              concat(
                // List all resources of type resourceSegment
                pathEndOrSingleSlash {
                  concat(
                    aggregate(request, Scope.Root),
                    list(request, Scope.Root)
                  )
                },
                // List all resources of type resourceSegment inside an organization
                (label & pathEndOrSingleSlash) { org =>
                  val scope = Scope.Org(org)
                  concat(
                    aggregate(request, scope),
                    list(request, scope)
                  )
                }
              )
            },
            projectRef { project =>
              projectContext(project) { pc =>
                // List all resources of type resourceSegment inside a project
                (mainIndexRequest(Some(pc), resourceSchema) & pathEndOrSingleSlash) { request =>
                  val scope = Scope.Project(project)
                  concat(
                    aggregate(request, scope),
                    list(request, scope)
                  )
                }
              }
            }
          )
        }
      }
    }.toSeq*)

  private def list(request: MainIndexRequest, scope: Scope)(using Caller): Route =
    (get & extractHttp4sUri) { uri =>
      given JsonLdEncoder[SearchResults[JsonObject]] =
        searchResultsJsonLdEncoder(ContextValue(contexts.searchMetadata), request.pagination, uri)
      emit {
        projectScopeResolver(scope, resources.read).flatMap { projects =>
          mainIndexQuery.list(request, projects)
        }
      }
    }

  private def aggregate(request: MainIndexRequest, scope: Scope)(using Caller): Route =
    (get & aggregated) {
      emitJson {
        projectScopeResolver(scope, resources.read).flatMap { projects =>
          mainIndexQuery.aggregate(request, projects)
        }
      }
    }

}
