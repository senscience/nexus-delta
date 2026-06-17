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
      resumer: ProjectDefResumer,
      projectionLifecycles: Set[ProjectProjectionLifecycle]
  ): IO[ProjectDefCoordinator] = {
    def fetchProjects(offset: Offset) =
      projects.states(offset).map(_.mapValue { p => ProjectDef(p.project, p.markedForDeletion) })

    apply(supervisor, fetchProjects, resumer, projectionLifecycles)
  }

  def apply(
      supervisor: Supervisor,
      fetchProjects: Offset => SuccessElemStream[ProjectDef],
      resumer: ProjectDefResumer,
      projectionLifecycles: Set[ProjectProjectionLifecycle]
  ): IO[ProjectDefCoordinator] = {
    val lifecyclesList = projectionLifecycles.toList
    val coordinator    = fromStream(supervisor, fetchProjects, resumer, lifecyclesList)

    val compiled  = CompiledProjection.fromStream(metadata, ExecutionStrategy.EveryNode, offset => coordinator(offset))
    val bootstrap = lifecyclesList.parUnorderedTraverse(_.bootstrap).void
    supervisor.run(compiled, bootstrap).as(coordinator)
  }

  def fromStream(
      supervisor: Supervisor,
      fetchProjects: Offset => SuccessElemStream[ProjectDef],
      resumer: ProjectDefResumer,
      projectionLifecycles: List[ProjectProjectionLifecycle]
  ): ProjectDefCoordinator = new ProjectDefCoordinator {
    override def apply(offset: Offset): ElemStream[Unit] = {
      val processProjects = fetchProjects(offset).evalMap {
        _.traverse {
          case p if p.markedForDeletion =>
            projectionLifecycles.traverse(b => b.compile(p.ref).flatMap(supervisor.destroy)).void
          case p                        =>
            startIfActive(p.ref)
        }
      }
      processProjects.concurrently(handleActivations)
    }

    /**
      * Background stream that resumes projections as activations come in: a whole-project activation resumes all of the
      * project's projections; a single-projection activation (e.g. from a user restart) resumes only the matching one.
      */
    private def handleActivations: Stream[IO, Unit] =
      resumer.run(resumeProject, resumeProjection)

    private def resumeProject(project: ProjectRef): IO[Unit] =
      logger.info(s"'$project' is active again, resuming its projections.") >> startProjections(project)

    // A single projection was (re)activated (e.g. a user restart): resume only the factory whose module matches, so we
    // don't restart the project's other projections.
    private def resumeProjection(metadata: ProjectionMetadata): IO[Unit] =
      projectionLifecycles
        .find(_.module == metadata.module)
        .traverse { factory =>
          metadata.project.traverse { project =>
            logger.info(s"Resuming projection '${metadata.name}' for project '$project'.") >> start(factory, project)
          }
        }
        .void

    // Start a project's projections from the state stream only if the project is currently active; otherwise they are
    // started when it next becomes active (via `handleActivations`). This avoids starting—then passivating—projections
    // for inactive projects each time the (every-node, offset-0) state stream is replayed.
    private def startIfActive(project: ProjectRef): IO[Unit] =
      resumer.isActive(project).flatMap {
        case true  => startProjections(project)
        case false => logger.debug(s"Project indexing for '$project' is not started as its project is not active.")
      }

    private def startProjections(project: ProjectRef): IO[Unit] =
      projectionLifecycles.traverse_(start(_, project))

    // `supervisor.run` is idempotent: if the projection is already supervised it is left running and the call is a
    // no-op. So we always call it rather than checking the current state with `describe` — that check would race with
    // the projection's natural lifecycle. Overlapping triggers (a project-state replay and an activation) thus collapse
    // to a single start.
    private def start(projectionLifecycle: ProjectProjectionLifecycle, project: ProjectRef): IO[Unit] =
      projectionLifecycle.compile(project).flatMap { compiled =>
        supervisor.run(compiled, projectionLifecycle.onInit(project)).void
      }
  }
}
