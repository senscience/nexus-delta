package ai.senscience.nexus.delta.testplugin

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.plugin.{Plugin, PluginDef}
import cats.effect.IO
import izumi.distage.model.Locator
import izumi.distage.model.definition.ModuleDef

case class TestPluginDef() extends PluginDef {

  override def module: ModuleDef =
    new ModuleDef {
      make[TestPlugin]
      make[TestPluginRoutes].from { (baseUri: BaseUri) =>
        new TestPluginRoutes(baseUri)
      }
      many[PriorityRoute].add((routes: TestPluginRoutes) =>
        PriorityRoute(1, routes.routes, requiresStrictEntity = true)
      )
    }

  override val info: PluginDescription = PluginDescription("testplugin", "0.1.0")

  override def initialize(locator: Locator): IO[Plugin] = IO.pure(locator.get[TestPlugin])

}
