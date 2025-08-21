package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMinPriority
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ViewsRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.views.ViewsList
import ai.senscience.nexus.delta.sdk.views.ViewsList.AggregateViewsList
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
import izumi.distage.model.definition.Id

object ViewsCommonModule extends NexusModuleDef {

  make[ProjectionsDirectives].from {
    (
        projections: Projections,
        projectionErrors: ProjectionErrors,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      ProjectionsDirectives(projections, projectionErrors)(baseUri, cr, ordering)
  }

  make[AggregateViewsList].from { (internal: Set[ViewsList]) =>
    new AggregateViewsList(internal.toList)
  }

  make[ViewsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        viewsList: AggregateViewsList,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new ViewsRoutes(
        identities,
        aclCheck,
        viewsList
      )(baseUri, cr, ordering)
  }

  many[PriorityRoute].add { (route: ViewsRoutes) =>
    PriorityRoute(pluginsMinPriority - 3, route.routes, requiresStrictEntity = true)
  }
}
