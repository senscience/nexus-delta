package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.*
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.ProjectSearchParams
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.projects.{create as CreateProjects, delete as DeleteProjects, read as ReadProjects, write as WriteProjects}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources.read as ReadResources
import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig.PrefixConfig
import ai.senscience.nexus.delta.sdk.projects.model.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.ProjectNotFound
import ai.senscience.nexus.delta.sdk.projects.{ProjectScopeResolver, Projects, ProjectsStatistics}
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Decoder
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

/**
  * The project routes
  */
final class ProjectsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    projects: Projects,
    projectScopeResolver: ProjectScopeResolver,
    projectsStatistics: ProjectsStatistics
)(using baseUri: BaseUri, prefixConfig: PrefixConfig)(using
    PaginationConfig,
    RemoteContextResolution,
    JsonKeyOrdering,
    FusionConfig,
    Tracer[IO]
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  private def projectsSearchParams(org: Option[Label])(using Caller): Directive1[ProjectSearchParams] = {
    (searchParams & parameter("label".?)).tmap { case (deprecated, rev, createdBy, updatedBy, label) =>
      val filter = projectScopeResolver.access(org.fold(Scope.root)(Scope.Org(_)), ReadProjects).memoize
      ProjectSearchParams(
        org,
        deprecated,
        rev,
        createdBy,
        updatedBy,
        label,
        proj => filter.flatMap(_.map(_.grant(proj.ref)))
      )
    }
  }

  private def emitMetadata(statusCode: StatusCode, io: IO[ProjectResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[ProjectResource]): Route =
    emitMetadata(StatusCodes.OK, io)

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("projects") {
        extractCaller { case given Caller =>
          concat(
            // List projects
            (get & pathEndOrSingleSlash & extractHttp4sUri & fromPaginated & projectsSearchParams(None) &
              sort[Project]) { (uri, pagination, params, order) =>
              given JsonLdEncoder[SearchResults[ProjectResource]] =
                searchResultsJsonLdEncoder(Project.context, pagination, uri)
              routeSpan("projects") {
                emit(projects.list(pagination, params, order).widen[SearchResults[ProjectResource]])
              }
            },
            projectRef { project =>
              val authorizeCreate        = authorizeFor(project, CreateProjects)
              val authorizeWrite         = authorizeFor(project, WriteProjects)
              val authorizeDelete        = authorizeFor(project, DeleteProjects)
              val authorizeRead          = authorizeFor(project, ReadProjects)
              val authorizeReadResources = authorizeFor(project, ReadResources)
              def decodeFields           = {
                given Decoder[ProjectFields] = ProjectFields.decoder(project, prefixConfig)
                entity(as[ProjectFields])
              }
              concat(
                routeSpan("projects/<str:org>/<str:project>") {
                  concat(
                    (put & pathEndOrSingleSlash & revParamOpt) {
                      case None      =>
                        // Create project
                        (authorizeCreate & decodeFields) { fields =>
                          emitMetadata(StatusCodes.Created, projects.create(project, fields))
                        }
                      case Some(rev) =>
                        // Update project
                        (authorizeWrite & decodeFields) { fields =>
                          emitMetadata(projects.update(project, rev, fields))
                        }
                    },
                    (get & pathEndOrSingleSlash & revParamOpt) {
                      case Some(rev) => // Fetch project at specific revision
                        authorizeRead {
                          emit(projects.fetchAt(project, rev))
                        }
                      case None      => // Fetch project
                        emitOrFusionRedirect(
                          project,
                          authorizeRead {
                            emit(projects.fetch(project))
                          }
                        )
                    },
                    // Deprecate/delete project
                    (delete & pathEndOrSingleSlash & revParam & parameter("prune".?(false))) {
                      case (rev, true)  =>
                        authorizeDelete {
                          emitMetadata(projects.delete(project, rev))
                        }
                      case (rev, false) =>
                        authorizeWrite {
                          emitMetadata(projects.deprecate(project, rev))
                        }
                    }
                  )
                },
                (pathPrefix("undeprecate") & put & authorizeWrite & revParam) { revision =>
                  routeSpan("projects/<str:org>/<str:project>/undeprecate") {
                    emit(projects.undeprecate(project, revision))
                  }
                },
                // Project statistics
                (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
                  routeSpan("projects/<str:org>/<str:project>/statistics") {
                    authorizeReadResources {
                      emit(
                        OptionT(projectsStatistics.get(project)).toRight(ProjectNotFound(project)).rethrowT
                      )
                    }
                  }
                }
              )
            },
            // list projects for an organization
            (get & label & pathEndOrSingleSlash) { org =>
              (extractHttp4sUri & fromPaginated & projectsSearchParams(Some(org)) & sort[Project]) {
                (uri, pagination, params, order) =>
                  given JsonLdEncoder[SearchResults[ProjectResource]] =
                    searchResultsJsonLdEncoder(Project.context, pagination, uri)
                  emit(projects.list(pagination, params, order).widen[SearchResults[ProjectResource]])
              }
            }
          )
        }
      }
    }
}

object ProjectsRoutes {

  /**
    * @return
    *   the [[Route]] for projects
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      projects: Projects,
      projectScopeResolver: ProjectScopeResolver,
      projectsStatistics: ProjectsStatistics
  )(using
      BaseUri,
      PaginationConfig,
      PrefixConfig,
      RemoteContextResolution,
      JsonKeyOrdering,
      FusionConfig,
      Tracer[IO]
  ): Route =
    new ProjectsRoutes(identities, aclCheck, projects, projectScopeResolver, projectsStatistics).routes

}
