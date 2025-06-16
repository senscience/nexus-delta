package ai.senscience.nexus.delta.plugins.elasticsearch

import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

object ElasticSearchPlugin extends Plugin {

  override def stop(): IO[Unit] = IO.unit
}
