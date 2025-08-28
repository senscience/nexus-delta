package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.offset.Offset
import cats.effect.IO
import fs2.Stream

trait ProjectionsRestartScheduler {

  def run(projectionStream: Stream[IO, String], fromOffset: Offset)(implicit subject: Subject): IO[Unit]

}

object ProjectionsRestartScheduler {

  def apply(projections: Projections): ProjectionsRestartScheduler = new ProjectionsRestartScheduler {
    override def run(projectionStream: Stream[IO, String], fromOffset: Offset)(implicit subject: Subject): IO[Unit] =
      projectionStream
        .evalMap { projectionName =>
          projections.scheduleRestart(projectionName, fromOffset)
        }
        .compile
        .drain
  }

}
