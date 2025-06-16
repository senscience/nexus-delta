package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

/**
  * The archive plugin entrypoint.
  */
object ArchivePlugin extends Plugin {

  override def stop(): IO[Unit] = IO.unit
}
