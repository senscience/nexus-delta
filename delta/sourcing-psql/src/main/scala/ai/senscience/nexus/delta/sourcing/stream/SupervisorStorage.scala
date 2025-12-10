package ai.senscience.nexus.delta.sourcing.stream

import cats.effect.IO
import cats.effect.std.AtomicCell
import fs2.Stream

/**
  * The supervisor storage to keep the references of the running projections.
  * @see
  *   https://typelevel.org/cats-effect/docs/std/atomic-cell
  */
private class SupervisorStorage private (underlying: AtomicCell[IO, Map[String, Supervised]]) {

  def get(projectionName: String): IO[Option[Supervised]] =
    underlying.get.map(_.get(projectionName))

  def values: Stream[IO, Supervised] = Stream.eval(underlying.get).flatMap { map =>
    Stream.iterable(map.values)
  }

  def update(projectionName: String)(f: Supervised => IO[Supervised]): IO[Option[Supervised]] =
    underlying.evalModify { map =>
      map.get(projectionName) match {
        case Some(supervised) =>
          f(supervised).map { value => (map + (projectionName -> value), Some(value)) }
        case None             =>
          IO.pure((map, None))
      }
    }

  def updateWith(projectionName: String)(f: Option[Supervised] => IO[Option[Supervised]]): IO[Option[Supervised]] =
    underlying.evalModify { map =>
      f(map.get(projectionName)).map {
        case Some(supervised) => (map + (projectionName -> supervised), Some(supervised))
        case None             => (map, None)
      }
    }

  def delete[A](projectionName: String)(f: Supervised => IO[A]): IO[Option[A]] =
    underlying.evalModify { map =>
      map.get(projectionName) match {
        case Some(supervised) =>
          f(supervised).map { value => (map - projectionName, Some(value)) }
        case None             =>
          IO.pure((map, None))
      }
    }
}

object SupervisorStorage {

  def apply(): IO[SupervisorStorage] =
    AtomicCell[IO].of(Map.empty[String, Supervised]).map(new SupervisorStorage(_))

}
