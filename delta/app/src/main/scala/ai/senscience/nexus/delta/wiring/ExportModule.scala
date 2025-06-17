package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.config.AppConfig
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ExportRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.exporter.Exporter
import izumi.distage.model.definition.{Id, ModuleDef}

// $COVERAGE-OFF$
object ExportModule extends ModuleDef {

  make[Exporter].fromEffect { (config: AppConfig, xas: Transactors) =>
    Exporter(config.`export`, xas)
  }

  make[ExportRoutes].from {
    (
        cfg: AppConfig,
        identities: Identities,
        aclCheck: AclCheck,
        exporter: Exporter,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new ExportRoutes(identities, aclCheck, exporter)(
        cfg.http.baseUri,
        cr,
        ordering
      )
  }

  many[PriorityRoute].add { (route: ExportRoutes) =>
    PriorityRoute(pluginsMaxPriority + 1, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
