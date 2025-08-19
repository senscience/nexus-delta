package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.{IndexingSupervisionRoutes, SupervisionRoutes}
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.{ProjectHealer, ProjectsHealth}
import ai.senscience.nexus.delta.sourcing.projections.ProjectionErrors
import ai.senscience.nexus.delta.sourcing.stream.{ProjectActivitySignals, Supervisor}
import izumi.distage.model.definition.ModuleDef

/**
  * Supervision module wiring config.
  */
// $COVERAGE-OFF$
object SupervisionModule extends ModuleDef {

  make[SupervisionRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        supervisor: Supervisor,
        baseUri: BaseUri,
        jo: JsonKeyOrdering,
        projectsHealth: ProjectsHealth,
        projectHealer: ProjectHealer,
        projectActivitySignals: ProjectActivitySignals
    ) =>
      new SupervisionRoutes(
        identities,
        aclCheck,
        supervisor.getRunningProjections(),
        projectsHealth,
        projectHealer,
        projectActivitySignals
      )(baseUri, jo)
  }

  many[PriorityRoute].add { (route: SupervisionRoutes) =>
    PriorityRoute(pluginsMaxPriority + 12, route.routes, requiresStrictEntity = true)
  }

  make[IndexingSupervisionRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        projectionErrors: ProjectionErrors,
        baseUri: BaseUri,
        jo: JsonKeyOrdering
    ) =>
      new IndexingSupervisionRoutes(
        identities,
        aclCheck,
        projectionErrors
      )(baseUri, jo)
  }

  many[PriorityRoute].add { (route: IndexingSupervisionRoutes) =>
    PriorityRoute(pluginsMaxPriority + 12, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
