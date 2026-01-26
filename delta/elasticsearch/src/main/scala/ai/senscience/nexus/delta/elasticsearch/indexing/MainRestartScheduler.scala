package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sourcing.Scope.Root
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectionsRestartScheduler
import cats.effect.IO
import fs2.Stream

/**
  * Job allowing to schedule a restart of main indices from the provided offset.
  *   - Projections with an offset smaller than the provided offset are also ignored
  */
trait MainRestartScheduler {

  def run(fromOffset: Offset)(using Subject): IO[Unit]

}

object MainRestartScheduler {

  private val logger = Logger[MainRestartScheduler]

  def apply(projects: Projects, restartScheduler: ProjectionsRestartScheduler): MainRestartScheduler =
    apply(projects.currentRefs(Root), restartScheduler)

  def apply(
      currentProjects: Stream[IO, ProjectRef],
      restartScheduler: ProjectionsRestartScheduler
  ): MainRestartScheduler =
    new MainRestartScheduler {
      override def run(fromOffset: Offset)(using Subject): IO[Unit] =
        logger.info(s"Starting reindexing all main views from $fromOffset") >>
          restartScheduler.run(projectionNameStream, fromOffset).timed.flatMap { case (duration, _) =>
            logger.info(s"All main views restarted reindexing in '${duration.toSeconds} seconds'")
          }

      private def projectionNameStream = currentProjects.map(mainIndexingProjection)
    }

}
