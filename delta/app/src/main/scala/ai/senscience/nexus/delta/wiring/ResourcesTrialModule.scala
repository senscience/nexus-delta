package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMinPriority
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ResourcesTrialRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.resources.{Resources, ResourcesTrial, ValidateResource}
import ai.senscience.nexus.delta.sdk.schemas.Schemas
import cats.effect.{Clock, IO}
import distage.ModuleDef
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Resources trial wiring
  */
object ResourcesTrialModule extends ModuleDef {

  make[ResourcesTrial].from {
    (
        resources: Resources,
        validate: ValidateResource,
        fetchContext: FetchContext,
        contextResolution: ResolverContextResolution,
        clock: Clock[IO],
        uuidF: UUIDF,
        tracer: Tracer[IO] @Id("resources")
    ) =>
      ResourcesTrial(
        resources.fetchState(_, _, None),
        validate,
        fetchContext,
        contextResolution,
        clock
      )(using uuidF)(using tracer)
  }

  make[ResourcesTrialRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        schemas: Schemas,
        resourcesTrial: ResourcesTrial,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("resources")
    ) =>
      ResourcesTrialRoutes(
        identities,
        aclCheck,
        schemas,
        resourcesTrial
      )(using baseUri, cr, ordering, tracer)
  }

  many[PriorityRoute].add { (route: ResourcesTrialRoutes) =>
    PriorityRoute(pluginsMinPriority - 1, route.routes, requiresStrictEntity = true)
  }

}
