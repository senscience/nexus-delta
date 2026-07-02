package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.config.DescriptionConfig
import ai.senscience.nexus.delta.dependency.PostgresServiceDependency
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.routes.VersionRoutes
import ai.senscience.nexus.delta.sdk.RouteEntry
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.RouteContext
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sourcing.Transactors
import izumi.distage.model.definition.ModuleDef

/**
  * Version module wiring config.
  */
// $COVERAGE-OFF$
object VersionModule extends ModuleDef {

  many[ServiceDependency].add { (xas: Transactors) => new PostgresServiceDependency(xas) }

  make[VersionRoutes].from {
    (
        description: DescriptionConfig,
        identities: Identities,
        aclCheck: AclCheck,
        plugins: List[PluginDescription],
        dependencies: Set[ServiceDependency],
        ctx: RouteContext
    ) =>
      VersionRoutes(identities, aclCheck, plugins, dependencies.toList, description)(using ctx)
  }

  many[RouteEntry].add { (route: VersionRoutes) =>
    RouteEntry(pluginsMaxPriority + 1, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
