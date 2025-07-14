package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMinPriority
import ai.senscience.nexus.delta.config.AppConfig
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.shacl.ValidateShacl
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ResourcesRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.IndexingAction.AggregateIndexingAction
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.metrics.ScopedEventMetricEncoder
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.ResolverResolution.ResourceResolution
import ai.senscience.nexus.delta.sdk.resolvers.{ResolverContextResolution, Resolvers, ResourceResolution}
import ai.senscience.nexus.delta.sdk.resources.*
import ai.senscience.nexus.delta.sdk.resources.Resources.{ResourceDefinition, ResourceLog}
import ai.senscience.nexus.delta.sdk.resources.model.{Resource, ResourceEvent}
import ai.senscience.nexus.delta.sdk.schemas.FetchSchema
import ai.senscience.nexus.delta.sdk.schemas.model.Schema
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sourcing.{ScopedEventLog, Transactors}
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.{Id, ModuleDef}

/**
  * Resources wiring
  */
object ResourcesModule extends ModuleDef {
  make[ResourceResolution[Schema]].from { (aclCheck: AclCheck, resolvers: Resolvers, fetchSchema: FetchSchema) =>
    ResourceResolution.schemaResource(aclCheck, resolvers, fetchSchema, excludeDeprecated = false)
  }

  make[ValidateResource].from {
    (resourceResolution: ResourceResolution[Schema], validateShacl: ValidateShacl, config: ResourcesConfig) =>
      val schemaClaimResolver = SchemaClaimResolver(resourceResolution, config.schemaEnforcement)
      ValidateResource(schemaClaimResolver, validateShacl)
  }

  make[ResourcesConfig].from { (config: AppConfig) => config.resources }

  make[DetectChange].from { (config: ResourcesConfig) => DetectChange(config.skipUpdateNoChange) }

  make[ResourceDefinition].from { (validateResource: ValidateResource, detectChange: DetectChange, clock: Clock[IO]) =>
    Resources.definition(validateResource, detectChange, clock)
  }

  make[ResourceLog].from { (scopedDefinition: ResourceDefinition, config: ResourcesConfig, xas: Transactors) =>
    ScopedEventLog(scopedDefinition, config.eventLog, xas)
  }

  make[FetchResource].from { (scopedLog: ResourceLog) =>
    FetchResource(scopedLog)
  }

  make[Resources].from {
    (
        resourceLog: ResourceLog,
        fetchContext: FetchContext,
        resolverContextResolution: ResolverContextResolution,
        uuidF: UUIDF
    ) =>
      ResourcesImpl(
        resourceLog,
        fetchContext,
        resolverContextResolution
      )(uuidF)
  }

  make[ResolverContextResolution].from {
    (
        aclCheck: AclCheck,
        resolvers: Resolvers,
        rcr: RemoteContextResolution @Id("aggregate"),
        fetchResource: FetchResource
    ) =>
      ResolverContextResolution(aclCheck, resolvers, rcr, fetchResource)
  }

  make[ResourcesRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        resources: Resources,
        indexingAction: AggregateIndexingAction,
        shift: Resource.Shift,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig
    ) =>
      new ResourcesRoutes(
        identities,
        aclCheck,
        resources,
        indexingAction(_, _, _)(shift)
      )(
        baseUri,
        cr,
        ordering,
        fusionConfig
      )
  }

  many[SseEncoder[?]].add { base: BaseUri => ResourceEvent.sseEncoder(base) }

  many[ScopedEventMetricEncoder[?]].add { ResourceEvent.resourceEventMetricEncoder }

  many[ApiMappings].add(Resources.mappings)

  many[PriorityRoute].add { (route: ResourcesRoutes) =>
    PriorityRoute(pluginsMinPriority - 2, route.routes, requiresStrictEntity = true)
  }

  make[Resource.Shift].from { (resources: Resources, base: BaseUri) =>
    Resource.shift(resources)(base)
  }

  many[ResourceShift[?, ?]].ref[Resource.Shift]

}
