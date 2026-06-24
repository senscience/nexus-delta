package ai.senscience.nexus.delta.projectdeletion

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.projectdeletion.ProjectDeletionRunner.{logger, ProjectDeletionCandidate}
import ai.senscience.nexus.delta.projectdeletion.model.ProjectDeletionConfig
import ai.senscience.nexus.delta.sdk.projects.model.ProjectState
import ai.senscience.nexus.delta.sdk.projects.{Projects, ProjectsStatistics}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, ExecutionStrategy, ProjectionMetadata, Supervisor}
import cats.effect.{Clock, IO}
import fs2.Stream

import java.time.Instant

/**
  * Periodically streams the existing projects and deletes the ones that [[ShouldDeleteProject]] selects.
  */
class ProjectDeletionRunner(
    config: ProjectDeletionConfig,
    candidates: Stream[IO, ProjectDeletionCandidate],
    fetchLastEventTime: ProjectRef => IO[Option[Instant]],
    deleteProject: (ProjectRef, Int) => IO[Unit],
    clock: Clock[IO]
) {

  private def lastEventTime(candidate: ProjectDeletionCandidate): IO[Option[Instant]] =
    fetchLastEventTime(candidate.ref).flatTap {
      case None    => logger.error(s"Statistics for project '${candidate.ref}' were not found")
      case Some(_) => IO.unit
    }

  private def delete(candidate: ProjectDeletionCandidate): IO[Unit] =
    deleteProject(candidate.ref, candidate.rev)
      .handleErrorWith(e => logger.error(s"Error deleting project '${candidate.ref}' from plugin: $e"))

  def run: IO[Unit] = {
    val shouldDeleteProject = ShouldDeleteProject(config, lastEventTime, clock)

    def possiblyDelete(candidate: ProjectDeletionCandidate): IO[Unit] =
      shouldDeleteProject(candidate).flatMap { bool => IO.whenA(bool)(delete(candidate)) }

    candidates.evalMap(possiblyDelete).compile.drain
  }
}

object ProjectDeletionRunner {

  private val logger = Logger[ProjectDeletionRunner]

  final case class ProjectDeletionCandidate(
      ref: ProjectRef,
      rev: Int,
      deprecated: Boolean,
      markedForDeletion: Boolean,
      updatedAt: Instant
  )

  object ProjectDeletionCandidate {
    def fromState(state: ProjectState): ProjectDeletionCandidate =
      ProjectDeletionCandidate(state.project, state.rev, state.deprecated, state.markedForDeletion, state.updatedAt)
  }

  private val projectionMetadata: ProjectionMetadata =
    ProjectionMetadata("system", "project-automatic-deletion", None, None)

  def apply(
      projects: Projects,
      config: ProjectDeletionConfig,
      projectStatistics: ProjectsStatistics,
      clock: Clock[IO]
  ): ProjectDeletionRunner = {
    val candidates: Stream[IO, ProjectDeletionCandidate] =
      projects.currentStates(Offset.start).map(elem => ProjectDeletionCandidate.fromState(elem.value))

    val fetchLastEventTime: ProjectRef => IO[Option[Instant]] =
      ref => projectStatistics.get(ref).map(_.map(_.lastEventTime))

    val deleteProject: (ProjectRef, Int) => IO[Unit] = { (ref, rev) =>
      given Subject = Identity.Anonymous
      projects.delete(ref, rev).void
    }

    new ProjectDeletionRunner(config, candidates, fetchLastEventTime, deleteProject, clock)
  }

  /**
    * Constructs a ProjectDeletionRunner process that is started in the supervisor.
    */
  def start(
      projects: Projects,
      config: ProjectDeletionConfig,
      projectStatistics: ProjectsStatistics,
      supervisor: Supervisor,
      clock: Clock[IO]
  ): IO[ProjectDeletionRunner] = {
    val runner = apply(projects, config, projectStatistics, clock)

    val continuousStream = Stream.fixedRate[IO](config.idleCheckPeriod).evalMap(_ => runner.run).drain

    val compiledProjection =
      CompiledProjection.fromStream(projectionMetadata, ExecutionStrategy.TransientSingleNode, _ => continuousStream)

    supervisor
      .run(compiledProjection)
      .map(_ => runner)
  }
}
