package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

object SearchPlugin extends Plugin {

  override def stop(): IO[Unit] = IO.unit
}
