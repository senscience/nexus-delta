package ai.senscience.nexus.delta.projectdeletion

import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

object ProjectDeletionPlugin extends Plugin {
  override def stop(): IO[Unit] = IO.unit
}
