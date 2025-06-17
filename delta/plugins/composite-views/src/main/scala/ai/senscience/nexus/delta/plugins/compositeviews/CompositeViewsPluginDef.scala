package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import ai.senscience.nexus.delta.sdk.plugin.{Plugin, PluginDef}
import cats.effect.IO
import izumi.distage.model.Locator
import izumi.distage.model.definition.ModuleDef

class CompositeViewsPluginDef extends PluginDef {

  override def module: ModuleDef = new CompositeViewsPluginModule(priority)

  override val info: PluginDescription = PluginDescription("composite-views", BuildInfo.version)

  override def initialize(locator: Locator): IO[Plugin] = IO.pure(CompositeViewsPlugin)

}
