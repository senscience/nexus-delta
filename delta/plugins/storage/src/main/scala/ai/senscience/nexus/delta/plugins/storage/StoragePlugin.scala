package ai.senscience.nexus.delta.plugins.storage

import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

object StoragePlugin extends Plugin {

  override def stop(): IO[Unit] = IO.unit
}
