package ai.senscience.nexus.delta.testplugin

import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.plugin.Plugin
import cats.effect.IO

@SuppressWarnings(Array("UnusedMethodParameter"))
class TestPlugin(baseUri: BaseUri) extends Plugin {
  override def stop(): IO[Unit] = IO.println(s"Stopping plugin with $baseUri")
}
