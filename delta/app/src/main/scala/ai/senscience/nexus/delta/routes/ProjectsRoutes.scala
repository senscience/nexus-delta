package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
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
import cats.implicits.*
import io.circe.Decoder
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.*

/**
  * The project routes
  */
final class ProjectsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    projects: Projects,
    projectScopeResolver: ProjectScopeResolver,
    projectsStatistics: ProjectsStatistics
)(implicit
    baseUri: BaseUri,
    paginationConfig: PaginationConfig,
    prefixConfig: PrefixConfig,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering,
    fusionConfig: FusionConfig
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  private def projectsSearchParams(org: Option[Label])(implicit caller: Caller): Directive1[ProjectSearchParams] = {
    (searchParams & parameter("label".?)).tmap { case (deprecated, rev, createdBy, updatedBy, label) =>
      val filter = projectScopeResolver.access(org.fold(Scope.root)(Scope.Org), ReadProjects).memoize
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

  private def revisionParam: Directive[Tuple1[Int]] = parameter("rev".as[Int])

  private def emitMetadata(statusCode: StatusCode, io: IO[ProjectResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[ProjectResource]): Route =
    emitMetadata(StatusCodes.OK, io)

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("projects") {
        extractCaller { implicit caller =>
          concat(
            // List projects
            (get & pathEndOrSingleSlash & extractHttp4sUri & fromPaginated & projectsSearchParams(None) &
              sort[Project]) { (uri, pagination, params, order) =>
              implicit val searchJsonLdEncoder: JsonLdEncoder[SearchResults[ProjectResource]] =
                searchResultsJsonLdEncoder(Project.context, pagination, uri)

              emit(projects.list(pagination, params, order).widen[SearchResults[ProjectResource]])
            },
            projectRef.apply { project =>
              implicit val projectFieldsDecoder: Decoder[ProjectFields] = ProjectFields.decoder(project, prefixConfig)
              concat(
                concat(
                  (put & pathEndOrSingleSlash) {
                    parameter("rev".as[Int].?) {
                      case None      =>
                        // Create project
                        authorizeFor(project, CreateProjects).apply {
                          entity(as[ProjectFields]) { fields =>
                            emitMetadata(StatusCodes.Created, projects.create(project, fields))
                          }
                        }
                      case Some(rev) =>
                        // Update project
                        authorizeFor(project, WriteProjects).apply {
                          entity(as[ProjectFields]) { fields =>
                            emitMetadata(projects.update(project, rev, fields))
                          }
                        }
                    }
                  },
                  (get & pathEndOrSingleSlash) {
                    parameter("rev".as[Int].?) {
                      case Some(rev) => // Fetch project at specific revision
                        authorizeFor(project, ReadProjects).apply {
                          emit(projects.fetchAt(project, rev))
                        }
                      case None      => // Fetch project
                        emitOrFusionRedirect(
                          project,
                          authorizeFor(project, ReadProjects).apply {
                            emit(projects.fetch(project))
                          }
                        )
                    }
                  },
                  // Deprecate/delete project
                  (delete & pathEndOrSingleSlash) {
                    parameters("rev".as[Int], "prune".?(false)) {
                      case (rev, true)  =>
                        authorizeFor(project, DeleteProjects).apply {
                          emitMetadata(projects.delete(project, rev))
                        }
                      case (rev, false) =>
                        authorizeFor(project, WriteProjects).apply {
                          emitMetadata(projects.deprecate(project, rev))
                        }
                    }
                  }
                ),
                (pathPrefix("undeprecate") & put & revisionParam) { revision =>
                  authorizeFor(project, WriteProjects).apply {
                    emit(projects.undeprecate(project, revision))
                  }
                },
                // Project statistics
                (pathPrefix("statistics") & get & pathEndOrSingleSlash) {
                  authorizeFor(project, ReadResources).apply {
                    val stats = projectsStatistics.get(project)
                    emit(OptionT(stats).toRight[ProjectRejection](ProjectNotFound(project)).rethrowT)
                  }
                }
              )
            },
            // list projects for an organization
            (get & label & pathEndOrSingleSlash) { org =>
              (extractHttp4sUri & fromPaginated & projectsSearchParams(Some(org)) & sort[Project]) {
                (uri, pagination, params, order) =>
                  implicit val searchJsonLdEncoder: JsonLdEncoder[SearchResults[ProjectResource]] =
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
  )(implicit
      baseUri: BaseUri,
      pagination: PaginationConfig,
      prefixConfig: PrefixConfig,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering,
      fusionConfig: FusionConfig
  ): Route =
    new ProjectsRoutes(identities, aclCheck, projects, projectScopeResolver, projectsStatistics).routes

}
