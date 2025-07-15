package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ExportRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.exporter.{ExportConfig, Exporter}
import izumi.distage.model.definition.Id

// $COVERAGE-OFF$
object ExportModule extends NexusModuleDef {

  makeConfig[ExportConfig]("app.export")

  make[Exporter].fromEffect { (config: ExportConfig, xas: Transactors) =>
    Exporter(config, xas)
  }

  make[ExportRoutes].from {
    (
        baseUri: BaseUri,
        identities: Identities,
        aclCheck: AclCheck,
        exporter: Exporter,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new ExportRoutes(identities, aclCheck, exporter)(baseUri, cr, ordering)
  }

  many[PriorityRoute].add { (route: ExportRoutes) =>
    PriorityRoute(pluginsMaxPriority + 1, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
