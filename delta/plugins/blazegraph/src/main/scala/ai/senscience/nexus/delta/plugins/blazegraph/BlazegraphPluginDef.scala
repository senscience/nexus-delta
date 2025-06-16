package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.sdk.plugin.{Plugin, PluginDef}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import izumi.distage.model.Locator
import izumi.distage.model.definition.ModuleDef

class BlazegraphPluginDef extends PluginDef {

  override def module: ModuleDef = new BlazegraphPluginModule(priority)

  override val info: PluginDescription = PluginDescription("blazegraph", BuildInfo.version)

  override def initialize(locator: Locator): IO[Plugin] = IO.pure(BlazegraphPlugin)

}
