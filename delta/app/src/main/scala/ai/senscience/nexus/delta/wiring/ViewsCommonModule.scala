package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMinPriority
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ViewsRoutes
import ai.senscience.nexus.delta.sdk.RouteEntry
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{ProjectionsDirectives, RouteContext}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.views.ViewsList
import ai.senscience.nexus.delta.sdk.views.ViewsList.AggregateViewsList
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
import cats.effect.IO
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

object ViewsCommonModule extends NexusModuleDef {

  makeTracer("views-list")

  make[ProjectionsDirectives].from {
    (
        projections: Projections,
        projectionErrors: ProjectionErrors,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      ProjectionsDirectives(projections, projectionErrors)(using baseUri, cr, ordering)
  }

  make[AggregateViewsList].from { (internal: Set[ViewsList]) =>
    new AggregateViewsList(internal.toList)
  }

  make[ViewsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        viewsList: AggregateViewsList,
        ctx: RouteContext,
        tracer: Tracer[IO] @Id("views-list")
    ) =>
      new ViewsRoutes(
        identities,
        aclCheck,
        viewsList
      )(using ctx, tracer)
  }

  many[RouteEntry].add { (route: ViewsRoutes) =>
    RouteEntry(pluginsMinPriority - 3, route.routes, requiresStrictEntity = true)
  }
}
