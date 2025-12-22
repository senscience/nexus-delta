package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, ElemStream, ExecutionStatus, ExecutionStrategy, ProjectionMetadata, SuccessElemStream, Supervisor}
import cats.effect.IO
import cats.syntax.all.*

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
      projectionFactories: Set[ProjectProjectionFactory]
  ): IO[ProjectDefCoordinator] = {
    def fetchProjects(offset: Offset) =
      projects.states(offset).map(_.mapValue { p => ProjectDef(p.project, p.markedForDeletion) })

    apply(supervisor, fetchProjects, projectionFactories)
  }

  def apply(
      supervisor: Supervisor,
      fetchProjects: Offset => SuccessElemStream[ProjectDef],
      projectionFactories: Set[ProjectProjectionFactory]
  ): IO[ProjectDefCoordinator] = {
    val factoriesList = projectionFactories.toList
    val coordinator   = fromStream(supervisor, fetchProjects, factoriesList)

    val compiled  = CompiledProjection.fromStream(metadata, ExecutionStrategy.EveryNode, offset => coordinator(offset))
    val bootstrap = factoriesList.parUnorderedTraverse(_.bootstrap).void
    supervisor.run(compiled, bootstrap).as(coordinator)
  }

  def fromStream(
      supervisor: Supervisor,
      fetchProjects: Offset => SuccessElemStream[ProjectDef],
      projectionFactories: List[ProjectProjectionFactory]
  ): ProjectDefCoordinator = new ProjectDefCoordinator {
    override def apply(offset: Offset): ElemStream[Unit] =
      fetchProjects(offset).evalMap {
        _.traverse {
          case p if p.markedForDeletion =>
            projectionFactories.traverse { b =>
              supervisor.destroy(b.name(p.ref))
            }.void
          case p                        =>
            projectionFactories.traverse { b =>
              start(b, p.ref)
            }.void
        }
      }

    private def start(projectionFactory: ProjectProjectionFactory, project: ProjectRef): IO[Unit] =
      for {
        compiled <- projectionFactory.compile(project)
        status   <- supervisor.describe(compiled.metadata.name)
        _        <- status match {
                      case Some(value) if value.status == ExecutionStatus.Running =>
                        logger.info(s"'${projectionFactory.name(project)}' of '$project' is already running.")
                      case _                                                      =>
                        supervisor.run(compiled)
                    }
      } yield ()
  }
}
