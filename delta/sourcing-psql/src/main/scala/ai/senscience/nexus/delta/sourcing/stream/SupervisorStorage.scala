package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import cats.effect.std.AtomicCell
import cats.effect.{IO, Resource}
import fs2.Stream

/**
  * The supervisor storage to keep the references of the running projections.
  * @see
  *   https://typelevel.org/cats-effect/docs/std/atomic-cell
  */
private class SupervisorStorage private (
    running: AtomicCell[IO, Map[String, Supervised]]
) {

  def get(projectionName: String): IO[Option[Supervised]] =
    running.get.map(_.get(projectionName))

  def values: Stream[IO, Supervised] = Stream.eval(running.get).flatMap { map =>
    Stream.iterable(map.values)
  }

  /** Snapshot of the metadata of the currently supervised projections. */
  def runningMetadata: IO[List[ProjectionMetadata]] =
    running.get.map(_.values.toList.map(_.metadata))

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
}

object SupervisorStorage {

  /**
    * Creates the storage and registers an observable gauge reporting the number of supervised projections per module.
    */
  def apply(metrics: ProjectionMetrics): Resource[IO, SupervisorStorage] =
    for {
      running <- Resource.eval(AtomicCell[IO].of(Map.empty[String, Supervised]))
      storage  = new SupervisorStorage(running)
      _       <- metrics.monitorRunning(storage.runningMetadata)
    } yield storage
}
