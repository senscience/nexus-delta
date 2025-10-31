package ai.senscience.nexus.delta.sourcing.otel

import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.{Gauge, Meter}

final case class ProjectionMetricsCollection(
    processedGauge: Gauge[IO, Long],
    discardedGauge: Gauge[IO, Long],
    failedGauge: Gauge[IO, Long]
)

object ProjectionMetricsCollection {

  def apply(meter: Meter[IO]): IO[ProjectionMetricsCollection] = {
    val processedGauge: IO[Gauge[IO, Long]] =
      meter
        .gauge("nexus.indexing.projections.processed.gauge")
        .withDescription("Gauge of processed elems.")
        .create

    val discardedGauge: IO[Gauge[IO, Long]] =
      meter
        .gauge("nexus.indexing.projections.discarded.gauge")
        .withDescription("Gauge of discarded elems.")
        .create
    val failedGauge: IO[Gauge[IO, Long]]    =
      meter
        .gauge("nexus.indexing.projections.failed.gauge")
        .withDescription("Gauge of failing elems.")
        .create

    (processedGauge, discardedGauge, failedGauge).mapN(new ProjectionMetricsCollection(_, _, _))
  }

}
