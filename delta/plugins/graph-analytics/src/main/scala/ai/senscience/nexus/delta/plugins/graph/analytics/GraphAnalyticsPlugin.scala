package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

object GraphAnalyticsPlugin extends Plugin {

  override def stop(): IO[Unit] = IO.unit
}
