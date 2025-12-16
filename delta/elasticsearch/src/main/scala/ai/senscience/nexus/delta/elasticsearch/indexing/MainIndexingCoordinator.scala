package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.main.MainIndexDef
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sdk.stream.MainDocumentStream
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.data.NonEmptyChain
import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.trace.Tracer

sealed trait MainIndexingCoordinator

object MainIndexingCoordinator {

  private[indexing] case class ProjectDef(ref: ProjectRef, markedForDeletion: Boolean)

  private val logger = Logger[MainIndexingCoordinator]

  /** If indexing is disabled we can only log */
  private case object Noop extends MainIndexingCoordinator {
    def log: IO[Unit] =
      logger.info("Main indexing has been disabled via config")

  }

  /**
    * Coordinates the main indexing as a projection per project
    *
    * @param fetchProjects
    *   stream of projects
    * @param mainDocumentStream
    *   to provide the data feeding the projections
    * @param supervisor
    *   the general supervisor
    */
  final private class Active(
      fetchProjects: Offset => ElemStream[ProjectDef],
      mainDocumentStream: MainDocumentStream,
      supervisor: Supervisor,
      sink: Sink
  ) extends MainIndexingCoordinator {

    def run(offset: Offset): ElemStream[Unit] =
      fetchProjects(offset).evalMap {
        _.traverse {
          case p if p.markedForDeletion => destroy(p.ref)
          case p                        => start(p.ref)
        }
      }

    private def compile(project: ProjectRef): IO[CompiledProjection] =
      IO.fromEither(
        CompiledProjection.compile(
          mainIndexingProjectionMetadata(project),
          ExecutionStrategy.PersistentSingleNode,
          Source(mainDocumentStream.continuous(project, _)),
          mainIndexingPipeline,
          sink
        )
      )

    // Start the main indexing projection for the given project
    private def start(project: ProjectRef): IO[Unit] =
      for {
        compiled <- compile(project)
        status   <- supervisor.describe(compiled.metadata.name)
        _        <- status match {
                      case Some(value) if value.status == ExecutionStatus.Running =>
                        logger.info(s"Main indexing of '$project' is already running.")
                      case _                                                      =>
                        supervisor.run(compiled)
                    }
      } yield ()

    // Destroy the indexing process for the given project
    private def destroy(project: ProjectRef): IO[Unit] =
      supervisor
        .destroy(
          mainIndexingProjection(project),
          logger.info(s"Project '$project' has been marked as deleted, stopping the main indexing...")
        )
        .void
  }

  val mainIndexingPipeline: NonEmptyChain[Operation] = NonEmptyChain(new MainDocumentToJson)

  val metadata: ProjectionMetadata = ProjectionMetadata("system", "main-indexing-coordinator", None, None)

  private def mainCoordinatorProjection(active: Active) =
    CompiledProjection.fromStream(metadata, ExecutionStrategy.EveryNode, offset => active.run(offset))

  def apply(
      projects: Projects,
      mainDocumentStream: MainDocumentStream,
      supervisor: Supervisor,
      client: ElasticSearchClient,
      mainIndex: MainIndexDef,
      batch: BatchConfig,
      indexingEnabled: Boolean
  )(using Tracer[IO]): IO[MainIndexingCoordinator] =
    if indexingEnabled then {
      val targetIndex = mainIndex.name

      def fetchProjects(offset: Offset) =
        projects.states(offset).map(_.map { p => ProjectDef(p.project, p.markedForDeletion) })

      val elasticsearchSink = ElasticSearchSink.mainIndexing(client, batch, targetIndex, Refresh.False)

      val init = client.createIndex(targetIndex, mainIndex.indexDef).void

      apply(fetchProjects, mainDocumentStream, supervisor, init, elasticsearchSink)
    } else {
      Noop.log.as(Noop)
    }

  def apply(
      fetchProjects: Offset => ElemStream[ProjectDef],
      mainDocumentStream: MainDocumentStream,
      supervisor: Supervisor,
      init: IO[Unit],
      sink: Sink
  ): IO[MainIndexingCoordinator] = {
    val coordinator = new Active(fetchProjects, mainDocumentStream, supervisor, sink)
    val compiled    = mainCoordinatorProjection(coordinator)
    supervisor.run(compiled, init).as(coordinator)
  }
}
