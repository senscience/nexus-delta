package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ResolversRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.*
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverEvent
import ai.senscience.nexus.delta.sdk.resources.FetchResource
import ai.senscience.nexus.delta.sdk.schemas.FetchSchema
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Resolvers wiring
  */
object ResolversModule extends NexusModuleDef {
  private given loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[ResolversConfig]("app.resolvers")

  makeTracer("resolvers")

  addRemoteContextResolution(contexts.definition)

  make[Resolvers].from {
    (
        fetchContext: FetchContext,
        resolverContextResolution: ResolverContextResolution,
        config: ResolversConfig,
        xas: Transactors,
        clock: Clock[IO],
        uuidF: UUIDF,
        tracer: Tracer[IO] @Id("resolvers")
    ) =>
      ResolversImpl(
        fetchContext,
        resolverContextResolution,
        ValidatePriority.priorityAlreadyExists(xas),
        config.eventLog,
        xas,
        clock
      )(using uuidF)(using tracer)
  }

  make[MultiResolution].from {
    (
        fetchContext: FetchContext,
        aclCheck: AclCheck,
        resolvers: Resolvers,
        fetchResource: FetchResource,
        fetchSchema: FetchSchema
    ) =>
      MultiResolution(
        fetchContext,
        aclCheck,
        resolvers,
        fetchResource,
        fetchSchema
      )
  }

  make[ResolversRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        resolvers: Resolvers,
        schemeDirectives: DeltaSchemeDirectives,
        multiResolution: MultiResolution,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig,
        tracer: Tracer[IO] @Id("resolvers")
    ) =>
      new ResolversRoutes(
        identities,
        aclCheck,
        resolvers,
        multiResolution,
        schemeDirectives
      )(using baseUri)(using
        cr,
        ordering,
        fusionConfig,
        tracer
      )
  }

  many[SseEncoder[?]].add { (base: BaseUri) => ResolverEvent.sseEncoder(base) }

  make[ResolverScopeInitialization].from {
    (
        resolvers: Resolvers,
        serviceAccount: ServiceAccount,
        config: ResolversConfig,
        tracer: Tracer[IO] @Id("resolvers")
    ) =>
      ResolverScopeInitialization(resolvers, serviceAccount, config.defaults)(using tracer)
  }
  many[ScopeInitialization].ref[ResolverScopeInitialization]

  many[ApiMappings].add(Resolvers.mappings)

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/resolvers-metadata.json"))

  many[PriorityRoute].add { (route: ResolversRoutes) =>
    PriorityRoute(pluginsMaxPriority + 9, route.routes, requiresStrictEntity = true)
  }
}
