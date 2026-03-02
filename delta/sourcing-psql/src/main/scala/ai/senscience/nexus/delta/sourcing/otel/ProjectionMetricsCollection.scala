package ai.senscience.nexus.delta.sourcing.otel

import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.{Gauge, Meter}

final case class ProjectionMetricsCollection(
    processed: Gauge[IO, Long],
    discarded: Gauge[IO, Long],
    failed: Gauge[IO, Long]
)

object ProjectionMetricsCollection {

  def apply(meter: Meter[IO]): IO[ProjectionMetricsCollection] = {
    val processed: IO[Gauge[IO, Long]] =
      meter
        .gauge("nexus.indexing.projections.processed")
        .withUnit("{elem}")
        .withDescription("Gauge of processed elems.")
        .create

    val discarded: IO[Gauge[IO, Long]] =
      meter
        .gauge("nexus.indexing.projections.discarded")
        .withUnit("{elem}")
        .withDescription("Gauge of discarded elems.")
        .create
    val failed: IO[Gauge[IO, Long]]    =
      meter
        .gauge("nexus.indexing.projections.failed")
        .withUnit("{elem}")
        .withDescription("Gauge of failing elems.")
        .create

    (processed, discarded, failed).mapN(new ProjectionMetricsCollection(_, _, _))
  }

}
