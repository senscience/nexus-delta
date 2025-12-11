package ai.senscience.nexus.delta.sourcing.stream

import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Channel

/**
  * The supervisor storage to keep the references of the running projections.
  * @see
  *   https://typelevel.org/cats-effect/docs/std/atomic-cell
  */
private class SupervisorStorage private (
    running: AtomicCell[IO, Map[String, Supervised]],
    failing: Channel[IO, String]
) {

  def get(projectionName: String): IO[Option[Supervised]] =
    running.get.map(_.get(projectionName))

  def values: Stream[IO, Supervised] = Stream.eval(running.get).flatMap { map =>
    Stream.iterable(map.values)
  }

  def failingStream: Stream[IO, String] = failing.stream

  def update(projectionName: String)(f: Supervised => IO[Supervised]): IO[Option[Supervised]] =
    running.evalModify { map =>
      map.get(projectionName) match {
        case Some(supervised) =>
          f(supervised).map { value => (map + (projectionName -> value), Some(value)) }
        case None             =>
          IO.pure((map, None))
      }
    }

  def updateWith(projectionName: String)(f: Option[Supervised] => IO[Option[Supervised]]): IO[Option[Supervised]] =
    running.evalModify { map =>
      f(map.get(projectionName)).map {
        case Some(supervised) => (map + (projectionName -> supervised), Some(supervised))
        case None             => (map, None)
      }
    }

  def delete[A](projectionName: String)(f: Supervised => IO[A]): IO[Option[A]] =
    running.evalModify { map =>
      map.get(projectionName) match {
        case Some(supervised) =>
          f(supervised).map { value => (map - projectionName, Some(value)) }
        case None             =>
          IO.pure((map, None))
      }
    }

  def sendFailing(projectionName: String): IO[Unit] = failing.send(projectionName).void
}

object SupervisorStorage {

  def apply(): IO[SupervisorStorage] =
    (AtomicCell[IO].of(Map.empty[String, Supervised]), Channel.bounded[IO, String](2_000)).mapN {
      new SupervisorStorage(_, _)
    }

}
