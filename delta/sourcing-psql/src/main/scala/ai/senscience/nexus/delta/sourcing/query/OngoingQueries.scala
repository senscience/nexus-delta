package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming.QueryStatus
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming.QueryStatus.Ongoing
import ai.senscience.nexus.delta.sourcing.query.OngoingQueries.Execute
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.Meter

import java.util.UUID

/**
  * Allows to limit the number of running queries at a given
  */
trait OngoingQueries {

  def tryRun[A](status: QueryStatus)(onRun: Execute[A], onWait: Execute[A]): IO[Option[(A, QueryStatus)]]

  def contains(uuid: UUID): IO[Boolean]

}

object OngoingQueries {

  private val logger = Logger[OngoingQueries]

  type Execute[A] = QueryStatus => IO[Option[(A, QueryStatus)]]

  case object Noop extends OngoingQueries {

    override def tryRun[A](status: QueryStatus)(onRun: Execute[A], onWait: Execute[A]): IO[Option[(A, QueryStatus)]] =
      onRun(status)

    override def contains(uuid: UUID): IO[Boolean] = IO.pure(false)
  }

  def apply(maxOngoing: Int)(using Meter[IO]): IO[OngoingQueries] = {
    val ongoingSet          = AtomicCell[IO].of(Set.empty[UUID])
    val ongoingQueriesGauge = Meter[IO]
      .gauge[Long]("nexus.indexing.ongoing.queries")
      .withDescription("Gauge of ongoing indexing queries.")
      .create
    (ongoingSet, ongoingQueriesGauge).mapN { case (values, gauge) =>
      new OngoingQueries {

        private def clean(queryStatus: QueryStatus) =
          values.updateAndGet(_ - queryStatus.uuid).flatMap { set => gauge.record(set.size) }

        override def tryRun[A](
            status: QueryStatus
        )(onRun: Execute[A], onWait: Execute[A]): IO[Option[(A, QueryStatus)]] = {
          values
            .evalModify { set =>
              if set.contains(status.uuid) || set.size < maxOngoing then
                logger
                  .trace(s"Query '${status.uuid}' can be run and is registered.")
                  .as((set + status.uuid, true))
                  .flatTap { case (set, _) => gauge.record(set.size) }
              else
                logger
                  .trace(s"Query '${status.uuid}' can't be run for the moment as there is no capacity (${set.size}).")
                  .as((set, false))
            }
            .flatMap {
              case true  =>
                onRun(status).guaranteeCase {
                  case Outcome.Succeeded(fa) =>
                    fa.flatMap {
                      case Some((_, _: Ongoing)) => IO.unit
                      case _                     => clean(status)
                    }
                  case Outcome.Errored(e)    => clean(status)
                  case Outcome.Canceled()    => clean(status)
                }
              case false => onWait(status)
            }
        }

        override def contains(uuid: UUID): IO[Boolean] = values.get.map(_.contains(uuid))
      }
    }
  }
}
