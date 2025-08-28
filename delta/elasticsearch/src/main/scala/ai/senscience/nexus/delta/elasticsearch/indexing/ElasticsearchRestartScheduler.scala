package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectionsRestartScheduler
import cats.effect.IO

/**
  * Job allowing to schedule a restart of all active views from the provided offset.
  *   - Deprecated views are ignored
  *   - Active views with an offset smaller than the provided offset are also ignored
  */
trait ElasticsearchRestartScheduler {

  def run(fromOffset: Offset)(implicit subject: Subject): IO[Unit]

}

object ElasticsearchRestartScheduler {

  private val logger = Logger[ElasticsearchRestartScheduler]

  def apply(
      currentActiveViews: CurrentActiveViews,
      restartScheduler: ProjectionsRestartScheduler
  ): ElasticsearchRestartScheduler =
    new ElasticsearchRestartScheduler {
      override def run(fromOffset: Offset)(implicit subject: Subject): IO[Unit] =
        logger.info(s"Starting reindexing all elasticsearch views from $fromOffset") >>
          restartScheduler.run(projectionNameStream, fromOffset).timed.flatMap { case (duration, _) =>
            logger.info(s"All elasticsearch views restarted reindexing in '${duration.toSeconds} seconds'")
          }

      private def projectionNameStream = currentActiveViews.stream.map(_.projection)
    }

}
