package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

object BlazegraphPlugin extends Plugin {

  override def stop(): IO[Unit] = IO.unit
}
