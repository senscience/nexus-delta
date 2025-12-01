package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming.QueryStatus
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming.QueryStatus.Ongoing
import cats.effect.std.Mutex
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.Meter

import java.util.UUID

trait OngoingQuerySet {

  def update(status: QueryStatus): IO[Unit]

  def delete(status: QueryStatus): IO[Unit]

  def runnable(status: QueryStatus): IO[Boolean]

}

object OngoingQuerySet {

  private val logger = Logger[OngoingQuerySet]

  case object Noop extends OngoingQuerySet {
    override def update(status: QueryStatus): IO[Unit] =
      IO.unit

    override def delete(status: QueryStatus): IO[Unit] =
      IO.unit

    override def runnable(status: QueryStatus): IO[Boolean] =
      IO.pure(true)
  }

  def apply(maxOngoing: Int)(using Meter[IO]): IO[OngoingQuerySet] = {
    val ongoingSet            = Ref.of[IO, Set[UUID]](Set.empty[UUID])
    val ongoingQueriesCounter = Meter[IO]
      .gauge[Long]("nexus.indexing.ongoing.queries")
      .withDescription("Gauge of ongoing indexing queries.")
      .create
    (ongoingSet, Mutex[IO], ongoingQueriesCounter).mapN { case (values, mutex, gauge) =>
      new OngoingQuerySet {

        private def add(uuid: UUID) =
          values.updateAndGet(_ + uuid).flatMap { set => gauge.record(set.size) }

        private def remove(uuid: UUID) =
          values.updateAndGet(_ - uuid).flatMap { set => gauge.record(set.size) }

        override def update(status: QueryStatus): IO[Unit] =
          mutex.lock.surround {
            status match {
              case Ongoing(uuid, _) =>
                logger.trace(s"Adding query $uuid as ongoing") >>
                  add(uuid)
              case other            =>
                logger.trace(s"Removing query ${other.uuid} as ongoing") >>
                  remove(other.uuid)
            }
          }

        override def delete(status: QueryStatus): IO[Unit] =
          mutex.lock.surround {
            logger.trace(s"Removing query ${status.uuid} as ongoing") >>
              remove(status.uuid)
          }

        override def runnable(status: QueryStatus): IO[Boolean] =
          mutex.lock.surround {
            values.get
              .map { set =>
                set.contains(status.uuid) || set.size < maxOngoing
              }
              .flatTap {
                case true  =>
                  logger.trace(s"Query ${status.uuid} can be run and is registered") >>
                    add(status.uuid)
                case false =>
                  logger.trace(s"Query ${status.uuid} can't be run for the moment as there is no capacity.")
              }
          }
      }
    }
  }
}
