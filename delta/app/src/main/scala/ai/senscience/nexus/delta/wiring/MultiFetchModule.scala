package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.MultiFetchRoutes
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.multifetch.MultiFetch
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchRequest
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sdk.{PriorityRoute, ResourceShifts}
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
        baseUri: BaseUri,
        rcr: RemoteContextResolution @Id("aggregate"),
        jko: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("multi-fetch")
    ) =>
      new MultiFetchRoutes(identities, aclCheck, multiFetch)(using baseUri)(using rcr, jko, tracer)
  }

  many[PriorityRoute].add { (route: MultiFetchRoutes) =>
    PriorityRoute(pluginsMaxPriority + 13, route.routes, requiresStrictEntity = true)
  }

}
