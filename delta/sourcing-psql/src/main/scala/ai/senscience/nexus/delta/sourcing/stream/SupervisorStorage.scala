package ai.senscience.nexus.delta.sourcing.stream

import cats.effect.{IO, Ref}
import fs2.Stream

/**
  * The supervisor storage to keep the references of the running projections
  */
private class SupervisorStorage private (underlying: Ref[IO, Map[String, Supervised]]) {

  def get(projectionName: String): IO[Option[Supervised]] =
    underlying.get.map(_.get(projectionName))

  def values: Stream[IO, Supervised] = Stream.eval(underlying.get).flatMap { map =>
    Stream.iterable(map.values)
  }

  def add(projectionName: String, supervised: Supervised): IO[Unit] =
    underlying.update(_ + (projectionName -> supervised))

  def update(projectionName: String, f: Supervised => Supervised): IO[Unit] =
    underlying.update(
      _.updatedWith(projectionName)(_.map(f))
    )

  def delete(projectionName: String): IO[Unit] = underlying.update(_ - projectionName)
}

object SupervisorStorage {

  def apply(): IO[SupervisorStorage] =
    Ref.of[IO, Map[String, Supervised]](Map.empty).map(new SupervisorStorage(_))

}
