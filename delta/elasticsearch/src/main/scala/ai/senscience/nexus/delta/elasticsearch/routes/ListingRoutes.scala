package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.model.contexts
import ai.senscience.nexus.delta.elasticsearch.query.{MainIndexQuery, MainIndexRequest}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaSchemeDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources
import ai.senscience.nexus.delta.sdk.projects.ProjectScopeResolver
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.Label
import akka.http.scaladsl.server.*
import io.circe.JsonObject
import io.circe.syntax.EncoderOps

class ListingRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    projectScopeResolver: ProjectScopeResolver,
    resourcesToSchemas: ResourceToSchemaMappings,
    schemeDirectives: DeltaSchemeDirectives,
    defaultIndexQuery: MainIndexQuery
)(implicit
    baseUri: BaseUri,
    paginationConfig: PaginationConfig,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with ElasticSearchViewsDirectives
    with RdfMarshalling {

  import schemeDirectives.*

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      concat(genericResourcesRoutes, resourcesListings)
    }

  private val genericResourcesRoutes: Route =
    pathPrefix("resources") {
      extractCaller { implicit caller =>
        concat(
          (searchParametersAndSortList & paginated) { (params, sort, page) =>
            val request = MainIndexRequest(params, page, sort)
            concat(
              // List/aggregate all resources
              pathEndOrSingleSlash {
                concat(
                  aggregate(request, Scope.Root),
                  list(request, Scope.Root)
                )
              },
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
            projectContext(project) { implicit pc =>
              (get & searchParametersInProject & paginated) { (params, sort, page) =>
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

  private val resourcesListings: Route =
    concat(resourcesToSchemas.value.map { case (Label(resourceSegment), resourceSchema) =>
      pathPrefix(resourceSegment) {
        extractCaller { implicit caller =>
          concat(
            (searchParametersAndSortList & paginated) { (params, sort, page) =>
              val request = MainIndexRequest(params.withSchema(resourceSchema), page, sort)
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
              projectContext(project) { implicit pc =>
                // List all resources of type resourceSegment inside a project
                (searchParametersInProject & paginated & pathEndOrSingleSlash) { (params, sort, page) =>
                  val request = MainIndexRequest(params.withSchema(resourceSchema), page, sort)
                  val scope   = Scope.Project(project)
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

  private def list(request: MainIndexRequest, scope: Scope)(implicit caller: Caller): Route =
    (get & paginated & extractHttp4sUri) { (page, uri) =>
      implicit val searchJsonLdEncoder: JsonLdEncoder[SearchResults[JsonObject]] =
        searchResultsJsonLdEncoder(ContextValue(contexts.searchMetadata), page, uri)
      emit {
        projectScopeResolver(scope, resources.read).flatMap { projects =>
          defaultIndexQuery.list(request, projects)
        }
      }
    }

  private def aggregate(request: MainIndexRequest, scope: Scope)(implicit caller: Caller): Route =
    (get & aggregated) {

      emit {
        projectScopeResolver(scope, resources.read).flatMap { projects =>
          defaultIndexQuery.aggregate(request, projects).map(_.asJson)
        }
      }

    }

}
