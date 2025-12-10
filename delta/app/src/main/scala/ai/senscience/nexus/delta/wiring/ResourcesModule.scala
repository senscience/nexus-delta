package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMinPriority
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.shacl.ValidateShacl
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ResourcesRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.indexing.SyncIndexingAction.AggregateIndexingAction
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.indexing.MainDocumentEncoder
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.metrics.ScopedEventMetricEncoder
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.ResolverResolution.ResourceResolution
import ai.senscience.nexus.delta.sdk.resolvers.{ResolverContextResolution, Resolvers, ResourceResolution}
import ai.senscience.nexus.delta.sdk.resources.*
import ai.senscience.nexus.delta.sdk.resources.Resources.ResourceLog
import ai.senscience.nexus.delta.sdk.resources.model.{Resource, ResourceEvent}
import ai.senscience.nexus.delta.sdk.schemas.FetchSchema
import ai.senscience.nexus.delta.sdk.schemas.model.Schema
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.{ScopedEventLog, Transactors}
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Resources wiring
  */
object ResourcesModule extends NexusModuleDef {
  makeConfig[ResourcesConfig]("app.resources")

  makeTracer("resources")

  make[ResourceResolution[Schema]].from { (aclCheck: AclCheck, resolvers: Resolvers, fetchSchema: FetchSchema) =>
    ResourceResolution.schemaResource(aclCheck, resolvers, fetchSchema, excludeDeprecated = false)
  }

  make[ValidateResource].from {
    (
        resourceResolution: ResourceResolution[Schema],
        validateShacl: ValidateShacl,
        config: ResourcesConfig,
        tracer: Tracer[IO] @Id("resources")
    ) =>
      given Tracer[IO]        = tracer
      val schemaClaimResolver = SchemaClaimResolver(resourceResolution, config.schemaEnforcement)
      ValidateResource(schemaClaimResolver, validateShacl)
  }

  make[ResourceLog].from {
    (
        validateResource: ValidateResource,
        clock: Clock[IO],
        config: ResourcesConfig,
        xas: Transactors,
        tracer: Tracer[IO] @Id("resources")
    ) =>
      val detectChange = DetectChange(config.skipUpdateNoChange)
      ScopedEventLog(Resources.definition(validateResource, detectChange, clock), config.eventLog, xas)(using tracer)
  }

  make[FetchResource].from { (scopedLog: ResourceLog) =>
    FetchResource(scopedLog)
  }

  make[Resources].from {
    (
        resourceLog: ResourceLog,
        fetchContext: FetchContext,
        resolverContextResolution: ResolverContextResolution,
        uuidF: UUIDF,
        tracer: Tracer[IO] @Id("resources")
    ) =>
      ResourcesImpl(
        resourceLog,
        fetchContext,
        resolverContextResolution
      )(using uuidF)(using tracer)
  }

  make[ResolverContextResolution].fromEffect {
    (
        aclCheck: AclCheck,
        resolvers: Resolvers,
        rcr: RemoteContextResolution @Id("aggregate"),
        fetchResource: FetchResource,
        config: ResourcesConfig,
        tracer: Tracer[IO] @Id("resolvers")
    ) =>
      ResolverContextResolution(aclCheck, resolvers, rcr, fetchResource, config.contextCache)(using tracer)
  }

  make[ResourcesRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        resources: Resources,
        indexingAction: AggregateIndexingAction,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig,
        tracer: Tracer[IO] @Id("resources")
    ) =>
      new ResourcesRoutes(
        identities,
        aclCheck,
        resources,
        indexingAction(Resources.entityType)(_, _, _)
      )(using baseUri)(using cr, ordering, fusionConfig, tracer)
  }

  many[SseEncoder[?]].add { (base: BaseUri) => ResourceEvent.sseEncoder(base) }

  many[ScopedEventMetricEncoder[?]].add { ResourceEvent.resourceEventMetricEncoder }

  many[ApiMappings].add(Resources.mappings)

  many[PriorityRoute].add { (route: ResourcesRoutes) =>
    PriorityRoute(pluginsMinPriority - 2, route.routes, requiresStrictEntity = true)
  }

  addIndexingType(Resources.entityType)

  many[ResourceShift[?, ?]].add { (resources: Resources, base: BaseUri) =>
    Resource.shift(resources)(base)
  }

  many[MainDocumentEncoder[?, ?]].add { (baseUri: BaseUri) =>
    Resource.mainDocumentEncoder(using baseUri)
  }

}
