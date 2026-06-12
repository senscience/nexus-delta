package ai.senscience.nexus.delta.elasticsearch.metrics

import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.Projections
import cats.effect.IO

/**
  * Schedules a restart of the (single, global) event metrics projection from the provided offset.
  */
trait EventMetricsRestartScheduler {

  def run(fromOffset: Offset)(using Subject): IO[Unit]

}

object EventMetricsRestartScheduler {

  def apply(projections: Projections): EventMetricsRestartScheduler =
    new EventMetricsRestartScheduler {
      override def run(fromOffset: Offset)(using Subject): IO[Unit] =
        projections.scheduleRestart(EventMetricsProjection.projectionMetadata, fromOffset)
    }

}
