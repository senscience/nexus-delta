package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.routes.MultiFetchRoutes
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.RouteContext
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.multifetch.MultiFetch
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchRequest
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sdk.{ResourceShifts, RouteEntry}
import cats.effect.IO
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

object MultiFetchModule extends NexusModuleDef {

  makeTracer("multi-fetch")

  make[MultiFetch].from {
    (
        aclCheck: AclCheck,
        shifts: ResourceShifts,
        tracer: Tracer[IO] @Id("multi-fetch")
    ) =>
      MultiFetch(
        aclCheck,
        (input: MultiFetchRequest.Input) => shifts.fetch(input.id, input.project)
      )(using tracer)
  }

  make[MultiFetchRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        multiFetch: MultiFetch,
        ctx: RouteContext,
        tracer: Tracer[IO] @Id("multi-fetch")
    ) =>
      new MultiFetchRoutes(identities, aclCheck, multiFetch)(using ctx, tracer)
  }

  many[RouteEntry].add { (route: MultiFetchRoutes) =>
    RouteEntry(
      pluginsMaxPriority + 13,
      route.routes,
      requiresStrictEntity = true,
      classifier = MultiFetchRoutes.classifier
    )
  }

}
