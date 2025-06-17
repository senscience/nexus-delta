package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import ai.senscience.nexus.delta.sdk.plugin.{Plugin, PluginDef}
import cats.effect.IO
import izumi.distage.model.Locator
import izumi.distage.model.definition.ModuleDef

class SearchPluginDef extends PluginDef {

  override def module: ModuleDef = new SearchPluginModule(priority)

  override val info: PluginDescription = PluginDescription("search", BuildInfo.version)

  override def initialize(locator: Locator): IO[Plugin] = IO.pure(SearchPlugin)

}
