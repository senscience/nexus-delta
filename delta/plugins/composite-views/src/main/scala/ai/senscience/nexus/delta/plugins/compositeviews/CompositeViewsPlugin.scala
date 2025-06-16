package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

object CompositeViewsPlugin extends Plugin {

  override def stop(): IO[Unit] = IO.unit
}
