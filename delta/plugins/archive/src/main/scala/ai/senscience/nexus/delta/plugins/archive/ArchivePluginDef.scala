package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import ai.senscience.nexus.delta.sdk.plugin.{Plugin, PluginDef}
import cats.effect.IO
import izumi.distage.model.Locator
import izumi.distage.model.definition.ModuleDef
class ArchivePluginDef extends PluginDef {

  override val module: ModuleDef = ArchivePluginModule

  override val info: PluginDescription = PluginDescription("archive", BuildInfo.version)

  override def initialize(locator: Locator): IO[Plugin] = IO.pure(ArchivePlugin)
}
