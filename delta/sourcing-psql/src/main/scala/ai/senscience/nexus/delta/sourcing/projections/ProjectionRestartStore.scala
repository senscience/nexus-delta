package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectionRestartStore.logger
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectionRestart
import ai.senscience.nexus.delta.sourcing.query.StreamingQuery
import cats.effect.IO
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import fs2.Stream
import io.circe.syntax.EncoderOps

import java.time.Instant

/**
  * Persistent operations for projections restart
  */
final class ProjectionRestartStore(xas: Transactors, config: QueryConfig) {

  def save(restart: ProjectionRestart): IO[Unit] =
    sql"""INSERT INTO public.projection_restarts (name, value, instant, acknowledged)
           |VALUES (${restart.name}, ${restart.asJson} ,${restart.instant}, false)
           |""".stripMargin.update.run
      .transact(xas.write)
      .void

  def acknowledge(id: Offset): IO[Unit] =
    sql"""UPDATE public.projection_restarts SET acknowledged = true
         |WHERE ordering = ${id.value}
         |""".stripMargin.update.run
      .transact(xas.write)
      .void

  def deleteExpired(instant: Instant): IO[Unit] =
    sql"""DELETE FROM public.projection_restarts WHERE instant < $instant""".update.run
      .transact(xas.write)
      .flatTap { deleted =>
        IO.whenA(deleted > 0)(logger.info(s"Deleted $deleted projection restarts."))
      }
      .void

  def stream(offset: Offset): Stream[IO, (Offset, ProjectionRestart)] =
    StreamingQuery[(Offset, ProjectionRestart)](
      offset,
      o => sql"""SELECT ordering, value, instant from public.projection_restarts
                  |WHERE ordering > $o and acknowledged = false
                  |ORDER BY ordering ASC
                  |LIMIT ${config.batchSize}""".stripMargin.query[(Offset, ProjectionRestart)],
      _._1,
      config.refreshStrategy,
      xas
    )
}

object ProjectionRestartStore {

  private val logger = Logger[ProjectionRestartStore]

}
