package ai.senscience.nexus.delta.sdk.deletion

import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategy}
import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig.DeletionConfig
import ai.senscience.nexus.delta.sdk.projects.model.ProjectState
import ai.senscience.nexus.delta.sdk.projects.{Projects, ProjectsConfig}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.partition.DatabasePartitioner
import ai.senscience.nexus.delta.sourcing.projections.ProjectLastUpdateStore
import ai.senscience.nexus.delta.sourcing.stream.*
import cats.effect.{Clock, IO}
import cats.syntax.all.*

/**
  * Stream to delete project from the system after those are marked as deleted
  */
sealed trait ProjectDeletionCoordinator

object ProjectDeletionCoordinator {

  private val logger = Logger[ProjectDeletionCoordinator]

  /**
    * If deletion is disabled, we do nothing
    */
  final private[deletion] case object Noop extends ProjectDeletionCoordinator

  /**
    * If deletion is enabled, we go through the project state log, looking for those marked for deletion
    */
  final private[deletion] class Active(
      fetchProjects: Offset => ElemStream[ProjectState],
      deletionTasks: List[ProjectDeletionTask],
      deletionConfig: DeletionConfig,
      serviceAccount: ServiceAccount,
      projectLastUpdateStore: ProjectLastUpdateStore,
      deletionStore: ProjectDeletionStore,
      clock: Clock[IO]
  ) extends ProjectDeletionCoordinator {

    implicit private val serviceAccountSubject: Subject = serviceAccount.subject

    def run(offset: Offset): ElemStream[Unit] =
      fetchProjects(offset).evalMap {
        _.traverse {
          case project if project.markedForDeletion =>
            // If it fails, we try again after a backoff
            val retryStrategy = RetryStrategy.retryOnNonFatal(
              deletionConfig.retryStrategy,
              logger,
              s"attempting to delete project ${project.project}"
            )
            delete(project).retry(retryStrategy)
          case _                                    => IO.unit
        }
      }

    private[deletion] def delete(project: ProjectState): IO[Unit] =
      for {
        _         <- logger.warn(s"Starting deletion of project ${project.project}")
        now       <- clock.realTimeInstant
        // Running preliminary tasks before deletion like deprecating and stopping views,
        // removing acl related to the project, etc...
        initReport = ProjectDeletionReport(project.project, project.updatedAt, now, project.updatedBy)
        report    <- deletionTasks
                       .foldLeftM(initReport) { case (report, task) =>
                         task(project.project).map(report ++ _)
                       }
        // Waiting for events issued by deletion tasks to be taken into account
        _         <- IO.sleep(deletionConfig.propagationDelay)
        // Delete the last updates for this project
        _         <- projectLastUpdateStore.delete(project.project)
        // Delete the events and states and save the deletion report
        _         <- deletionStore.deleteAndSaveReport(report)
        _         <- logger.info(s"Project ${project.project} has been successfully deleted.")
      } yield ()

    private[deletion] def list(project: ProjectRef): IO[List[ProjectDeletionReport]] =
      deletionStore.list(project)
  }

  /**
    * Build the project deletion stream according to the configuration
    */
  def apply(
      projects: Projects,
      databasePartitioner: DatabasePartitioner,
      deletionTasks: Set[ProjectDeletionTask],
      deletionConfig: ProjectsConfig.DeletionConfig,
      serviceAccount: ServiceAccount,
      projectLastUpdateStore: ProjectLastUpdateStore,
      xas: Transactors,
      clock: Clock[IO]
  ): ProjectDeletionCoordinator =
    if (deletionConfig.enabled) {
      new Active(
        projects.states,
        deletionTasks.toList,
        deletionConfig,
        serviceAccount,
        projectLastUpdateStore,
        new ProjectDeletionStore(xas, databasePartitioner),
        clock
      )
    } else
      Noop

  /**
    * Build and run the project deletion stream in the supervisor
    */
  // $COVERAGE-OFF$
  def apply(
      projects: Projects,
      databasePartitioner: DatabasePartitioner,
      deletionTasks: Set[ProjectDeletionTask],
      deletionConfig: ProjectsConfig.DeletionConfig,
      serviceAccount: ServiceAccount,
      supervisor: Supervisor,
      projectLastUpdateStore: ProjectLastUpdateStore,
      xas: Transactors,
      clock: Clock[IO]
  ): IO[ProjectDeletionCoordinator] = {
    val stream = apply(
      projects,
      databasePartitioner,
      deletionTasks,
      deletionConfig,
      serviceAccount,
      projectLastUpdateStore,
      xas,
      clock
    )
    stream match {
      case Noop           => logger.info("Projection deletion is disabled.").as(Noop)
      case active: Active =>
        val metadata: ProjectionMetadata = ProjectionMetadata("system", "project-deletion", None, None)
        supervisor
          .run(
            CompiledProjection.fromStream(
              metadata,
              ExecutionStrategy.PersistentSingleNode,
              active.run
            )
          )
          .as(active)
    }
  }
  // $COVERAGE-ON$
}
