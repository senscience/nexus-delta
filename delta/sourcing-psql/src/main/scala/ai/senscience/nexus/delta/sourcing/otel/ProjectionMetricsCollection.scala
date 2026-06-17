package ai.senscience.nexus.delta.sourcing.otel

import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.{Counter, Meter}

final case class ProjectionMetricsCollection(
    processed: Counter[IO, Long],
    discarded: Counter[IO, Long],
    failed: Counter[IO, Long],
    terminated: Counter[IO, Long],
    activations: Counter[IO, Long]
)

object ProjectionMetricsCollection {

  def apply(meter: Meter[IO]): IO[ProjectionMetricsCollection] = {
    val processed: IO[Counter[IO, Long]] =
      meter
        .counter[Long]("nexus.indexing.projections.progress.processed")
        .withUnit("{elem}")
        .withDescription("Count of processed elems, by module.")
        .create

    val discarded: IO[Counter[IO, Long]] =
      meter
        .counter[Long]("nexus.indexing.projections.progress.discarded")
        .withUnit("{elem}")
        .withDescription("Count of discarded elems, by module.")
        .create
    val failed: IO[Counter[IO, Long]]    =
      meter
        .counter[Long]("nexus.indexing.projections.progress.failed")
        .withUnit("{elem}")
        .withDescription("Count of failed elems, by module.")
        .create

    val terminated: IO[Counter[IO, Long]] =
      meter
        .counter[Long]("nexus.indexing.projections.terminated")
        .withUnit("{projection}")
        .withDescription("Count of projection terminations, by module and outcome (completed or failed).")
        .create

    val activations: IO[Counter[IO, Long]] =
      meter
        .counter[Long]("nexus.indexing.projections.activations")
        .withUnit("{activation}")
        .withDescription("Count of published activations that trigger projections to resume, by kind.")
        .create

    (processed, discarded, failed, terminated, activations).mapN(new ProjectionMetricsCollection(_, _, _, _, _))
  }

}
