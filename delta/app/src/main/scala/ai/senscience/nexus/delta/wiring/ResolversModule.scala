package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
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

/**
  * Resolvers wiring
  */
object ResolversModule extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[ResolversConfig]("app.resolvers")

  make[Resolvers].from {
    (
        fetchContext: FetchContext,
        resolverContextResolution: ResolverContextResolution,
        config: ResolversConfig,
        xas: Transactors,
        clock: Clock[IO],
        uuidF: UUIDF
    ) =>
      ResolversImpl(
        fetchContext,
        resolverContextResolution,
        ValidatePriority.priorityAlreadyExists(xas),
        config.eventLog,
        xas,
        clock
      )(uuidF)
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
        fusionConfig: FusionConfig
    ) =>
      new ResolversRoutes(
        identities,
        aclCheck,
        resolvers,
        multiResolution,
        schemeDirectives
      )(
        baseUri,
        cr,
        ordering,
        fusionConfig
      )
  }

  many[SseEncoder[?]].add { (base: BaseUri) => ResolverEvent.sseEncoder(base) }

  make[ResolverScopeInitialization].from {
    (resolvers: Resolvers, serviceAccount: ServiceAccount, config: ResolversConfig) =>
      ResolverScopeInitialization(resolvers, serviceAccount, config.defaults)
  }
  many[ScopeInitialization].ref[ResolverScopeInitialization]

  many[ApiMappings].add(Resolvers.mappings)

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/resolvers-metadata.json"))

  many[RemoteContextResolution].addEffect(
    for {
      resolversCtx     <- ContextValue.fromFile("contexts/resolvers.json")
      resolversMetaCtx <- ContextValue.fromFile("contexts/resolvers-metadata.json")
    } yield RemoteContextResolution.fixed(
      contexts.resolvers         -> resolversCtx,
      contexts.resolversMetadata -> resolversMetaCtx
    )
  )
  many[PriorityRoute].add { (route: ResolversRoutes) =>
    PriorityRoute(pluginsMaxPriority + 9, route.routes, requiresStrictEntity = true)
  }
}
