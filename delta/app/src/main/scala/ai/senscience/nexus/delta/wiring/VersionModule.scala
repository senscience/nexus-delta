package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.config.DescriptionConfig
import ai.senscience.nexus.delta.dependency.PostgresServiceDependency
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.VersionRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sourcing.Transactors
import izumi.distage.model.definition.{Id, ModuleDef}

/**
  * Version module wiring config.
  */
// $COVERAGE-OFF$
object VersionModule extends ModuleDef {

  many[ServiceDependency].add { (xas: Transactors) => new PostgresServiceDependency(xas) }

  make[VersionRoutes].from {
    (
        baseUri: BaseUri,
        description: DescriptionConfig,
        identities: Identities,
        aclCheck: AclCheck,
        plugins: List[PluginDescription],
        dependencies: Set[ServiceDependency],
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      VersionRoutes(identities, aclCheck, plugins, dependencies.toList, description)(
        baseUri,
        cr,
        ordering
      )
  }

  many[PriorityRoute].add { (route: VersionRoutes) =>
    PriorityRoute(pluginsMaxPriority + 1, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
