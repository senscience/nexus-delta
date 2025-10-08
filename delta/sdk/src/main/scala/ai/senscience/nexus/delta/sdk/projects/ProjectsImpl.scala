package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.search.Pagination
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.model.search.{SearchParams, SearchResults}
import ai.senscience.nexus.delta.sdk.organizations.FetchActiveOrganization
import ai.senscience.nexus.delta.sdk.projects.ProjectsImpl.{logger, ProjectsLog}
import ai.senscience.nexus.delta.sdk.projects.model.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectCommand.*
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.SuccessElemStream
import ai.senscience.nexus.delta.sourcing.{Scope, ScopedEventLog, Transactors}
import cats.effect.{Clock, IO}
import fs2.Stream
import org.typelevel.otel4s.trace.Tracer

final class ProjectsImpl private (
    log: ProjectsLog,
    scopeInitializer: ScopeInitializer,
    override val defaultApiMappings: ApiMappings
)(using Tracer[IO])
    extends Projects {

  override def create(
      ref: ProjectRef,
      fields: ProjectFields
  )(implicit caller: Subject): IO[ProjectResource] =
    for {
      resource <- eval(CreateProject(ref, fields, caller)).surround("createProject")
      _        <- scopeInitializer
                    .initializeProject(resource.value.ref)
                    .surround("initializeProject")
    } yield resource

  override def update(ref: ProjectRef, rev: Int, fields: ProjectFields)(implicit
      caller: Subject
  ): IO[ProjectResource] =
    eval(UpdateProject(ref, fields, rev, caller)).surround("updateProject")

  override def deprecate(ref: ProjectRef, rev: Int)(implicit caller: Subject): IO[ProjectResource] =
    eval(DeprecateProject(ref, rev, caller)).surround("deprecateProject") <*
      logger.info(s"Project '$ref' has been deprecated.")

  override def undeprecate(ref: ProjectRef, rev: Int)(implicit caller: Subject): IO[ProjectResource] = {
    eval(UndeprecateProject(ref, rev, caller)).surround("undeprecateProject") <*
      logger.info(s"Project '$ref' has been undeprecated.")
  }

  override def delete(ref: ProjectRef, rev: Int)(implicit caller: Subject): IO[ProjectResource] =
    eval(DeleteProject(ref, rev, caller)).surround("deleteProject") <*
      logger.info(s"Project '$ref' has been marked as deleted.")

  override def fetch(ref: ProjectRef): IO[ProjectResource] =
    log
      .stateOr(ref, ref, ProjectNotFound(ref))
      .map(_.toResource(defaultApiMappings))
      .surround("fetchProject")

  override def fetchAt(ref: ProjectRef, rev: Int): IO[ProjectResource] =
    log
      .stateOr(ref, ref, rev, ProjectNotFound(ref), RevisionNotFound(_, _))
      .map(_.toResource(defaultApiMappings))
      .surround("fetchProjectAt")

  override def fetchProject(ref: ProjectRef): IO[Project] = fetch(ref).map(_.value)

  override def list(
      pagination: Pagination.FromPagination,
      params: SearchParams.ProjectSearchParams,
      ordering: Ordering[ProjectResource]
  ): IO[SearchResults.UnscoredSearchResults[ProjectResource]] =
    SearchResults(
      log
        .currentStates(params.organization.fold(Scope.root)(Scope.Org(_)), _.toResource(defaultApiMappings))
        .evalFilter(params.matches),
      pagination,
      ordering
    ).surround("listProjects")

  override def currentRefs(scope: Scope): Stream[IO, ProjectRef] =
    log.currentStates(scope, _.project)

  override def states(offset: Offset): SuccessElemStream[ProjectState] = log.states(Scope.root, offset)

  private def eval(cmd: ProjectCommand): IO[ProjectResource] =
    log.evaluate(cmd.ref, cmd.ref, cmd).map(_._2.toResource(defaultApiMappings))

}

object ProjectsImpl {

  type ProjectsLog =
    ScopedEventLog[ProjectRef, ProjectState, ProjectCommand, ProjectEvent, ProjectRejection]

  private val logger = Logger[ProjectsImpl]

  /**
    * Constructs a [[Projects]] instance.
    */
  final def apply(
      fetchActiveOrg: FetchActiveOrganization,
      onCreate: ProjectRef => IO[Unit],
      validateDeletion: ValidateProjectDeletion,
      scopeInitializer: ScopeInitializer,
      defaultApiMappings: ApiMappings,
      config: EventLogConfig,
      xas: Transactors,
      clock: Clock[IO]
  )(using UUIDF, Tracer[IO]): Projects =
    new ProjectsImpl(
      ScopedEventLog(Projects.definition(fetchActiveOrg, onCreate, validateDeletion, clock), config, xas),
      scopeInitializer,
      defaultApiMappings
    )
}
