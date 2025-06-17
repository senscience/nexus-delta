package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.sourcing.PurgeElemFailures.logger
import ai.senscience.nexus.delta.sourcing.config.PurgeConfig
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.kernel.Logger
import doobie.postgres.implicits.*
import doobie.syntax.all.*

import java.time.Instant

final class PurgeElemFailures private[sourcing] (xas: Transactors) {

  /**
    * Deletes the projection errors that are older than the given instant.
    */
  def apply(instant: Instant): IO[Unit] =
    sql"""DELETE FROM public.failed_elem_logs WHERE instant < $instant""".stripMargin.update.run
      .transact(xas.write)
      .flatMap { deleted =>
        IO.whenA(deleted > 0)(logger.info(s"Deleted $deleted old projection failures."))
      }
}

object PurgeElemFailures {

  private val logger   = Logger[PurgeElemFailures]
  private val metadata = ProjectionMetadata("system", "delete-old-failed-elem", None, None)

  /**
    * Creates a [[PurgeProjection]] to schedule in the supervisor the deletion of old projection errors.
    */
  def apply(config: PurgeConfig, xas: Transactors): PurgeProjection = {
    val purgeElemFailures = new PurgeElemFailures(xas)
    PurgeProjection(metadata, config, purgeElemFailures.apply)
  }

}
