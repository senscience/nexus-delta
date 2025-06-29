package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.DeleteExpired.logger
import ai.senscience.nexus.delta.sourcing.config.PurgeConfig
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import cats.effect.IO
import doobie.postgres.implicits.*
import doobie.syntax.all.*

import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
  * Allow to delete expired ephemeral states
  */
final class DeleteExpired private[sourcing] (xas: Transactors) {

  def apply(instant: Instant): IO[Unit] = {
    for {
      deleted <- sql"""
                  | DELETE FROM public.ephemeral_states
                  | WHERE expires < $instant
                  """.stripMargin.update.run.transact(xas.write)
      _       <- IO.whenA(deleted > 0)(logger.info(s"Deleted $deleted expired ephemeral states"))
    } yield ()
  }
}

object DeleteExpired {
  private val logger = Logger[DeleteExpired]

  private val metadata: ProjectionMetadata = ProjectionMetadata("system", "delete-expired", None, None)

  /**
    * Creates a [[PurgeProjection]] instance so that it can be scheduled in the supervisor
    */
  def apply(deleteExpiredEvery: FiniteDuration, xas: Transactors): PurgeProjection = {
    // The ttl is defined in the ephemeral state via the expire column
    val purgeConfig   = PurgeConfig(deleteExpiredEvery, 0.second)
    val deleteExpired = new DeleteExpired(xas)
    PurgeProjection(metadata, purgeConfig, deleteExpired.apply)
  }
}
