package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream

trait ProjectDefCoordinator {

  def apply(offset: Offset): ElemStream[Unit]
}

object ProjectDefCoordinator {

  private val logger = Logger[ProjectDefCoordinator]

  val metadata: ProjectionMetadata = ProjectionMetadata("system", "project-def-coordinator", None, None)

  final case class ProjectDef(ref: ProjectRef, markedForDeletion: Boolean)

  def apply(
      supervisor: Supervisor,
      projects: Projects,
      projectActivity: ProjectActivity,
      projectionFactories: Set[ProjectProjectionFactory]
  ): IO[ProjectDefCoordinator] = {
    def fetchProjects(offset: Offset) =
      projects.states(offset).map(_.mapValue { p => ProjectDef(p.project, p.markedForDeletion) })

    apply(supervisor, fetchProjects, projectActivity, projectionFactories)
  }

  def apply(
      supervisor: Supervisor,
      fetchProjects: Offset => SuccessElemStream[ProjectDef],
      projectActivity: ProjectActivity,
      projectionFactories: Set[ProjectProjectionFactory]
  ): IO[ProjectDefCoordinator] = {
    val factoriesList = projectionFactories.toList
    val coordinator   = fromStream(supervisor, fetchProjects, projectActivity, factoriesList)

    val compiled  = CompiledProjection.fromStream(metadata, ExecutionStrategy.EveryNode, offset => coordinator(offset))
    val bootstrap = factoriesList.parUnorderedTraverse(_.bootstrap).void
    supervisor.run(compiled, bootstrap).as(coordinator)
  }

  def fromStream(
      supervisor: Supervisor,
      fetchProjects: Offset => SuccessElemStream[ProjectDef],
      projectActivity: ProjectActivity,
      projectionFactories: List[ProjectProjectionFactory]
  ): ProjectDefCoordinator = new ProjectDefCoordinator {
    override def apply(offset: Offset): ElemStream[Unit] = {
      val processProjects = fetchProjects(offset).evalMap {
        _.traverse {
          case p if p.markedForDeletion =>
            projectionFactories.traverse(b => b.compile(p.ref).flatMap(supervisor.destroy)).void
          case p                        =>
            startIfActive(p.ref)
        }
      }
      processProjects.concurrently(handleActivations)
    }

    /**
      * Background stream that revives the project's per-factory projections when the project becomes active again. The
      * activation is itself the signal that the project is active, so we start its projections directly.
      */
    private def handleActivations: Stream[IO, Unit] =
      projectActivity.activations.evalMap(startProjections)

    // Start a project's projections from the state stream only if the project is currently active; otherwise they are
    // started when it next becomes active (via `handleActivations`). This avoids starting—then passivating—projections
    // for inactive projects each time the (every-node, offset-0) state stream is replayed.
    private def startIfActive(project: ProjectRef): IO[Unit] =
      projectActivity.isActive(project).flatMap {
        case true  => startProjections(project)
        case false => logger.debug(s"Main indexing for '$project' is not started as its project is not active.")
      }

    private def startProjections(project: ProjectRef): IO[Unit] =
      projectionFactories.traverse_(start(_, project))

    // `supervisor.run` is atomic and idempotent (it stops any existing entry under the same name before starting),
    // so we always call it rather than checking the current state with `describe` — that check would race with the
    // projection's natural lifecycle. The cost is a possible no-op restart of an already-running projection, which is
    // harmless and bounded by the project state event rate.
    private def start(projectionFactory: ProjectProjectionFactory, project: ProjectRef): IO[Unit] =
      projectionFactory.compile(project).flatMap { compiled =>
        supervisor.run(compiled, projectionFactory.onInit(project)).void
      }
  }
}
