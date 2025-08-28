package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.config.BlazegraphViewsConfig
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.{CurrentActiveViews, SparqlCoordinator, SparqlProjectionLifeCycle, SparqlRestartScheduler}
import ai.senscience.nexus.delta.plugins.blazegraph.model.{contexts, BlazegraphViewEvent}
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks.Queries
import ai.senscience.nexus.delta.plugins.blazegraph.routes.{BlazegraphViewsIndexingRoutes, BlazegraphViewsRoutes, BlazegraphViewsRoutesHandler, SparqlSupervisionRoutes}
import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.{SparqlSlowQueryLogger, SparqlSlowQueryStore}
import ai.senscience.nexus.delta.plugins.blazegraph.supervision.{BlazegraphViewByNamespace, SparqlSupervision}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.directives.{DeltaSchemeDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.indexing.IndexingAction
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.ViewsList
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.projections.ProjectionsRestartScheduler
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import ai.senscience.nexus.delta.sourcing.stream.{ReferenceRegistry, Supervisor}
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id

/**
  * Blazegraph plugin wiring
  */
class BlazegraphPluginModule(priority: Int) extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[BlazegraphViewsConfig]("plugins.blazegraph")

  make[SparqlSlowQueryStore].from { (xas: Transactors) => SparqlSlowQueryStore(xas) }

  many[PurgeProjection].add { (config: BlazegraphViewsConfig, store: SparqlSlowQueryStore) =>
    SparqlSlowQueryStore.deleteExpired(config.slowQueries.purge, store)
  }

  make[SparqlSlowQueryLogger].from { (cfg: BlazegraphViewsConfig, store: SparqlSlowQueryStore, clock: Clock[IO]) =>
    SparqlSlowQueryLogger(store, cfg.slowQueries.slowQueryThreshold, clock)
  }

  make[SparqlClient].named("sparql-indexing-client").fromResource { (cfg: BlazegraphViewsConfig) =>
    SparqlClient(cfg.sparqlTarget, cfg.base, cfg.queryTimeout, cfg.credentials)
  }

  make[SparqlClient].named("sparql-query-client").fromResource { (cfg: BlazegraphViewsConfig) =>
    SparqlClient(cfg.sparqlTarget, cfg.base, cfg.queryTimeout, cfg.credentials)
  }

  make[ValidateBlazegraphView].from {
    (
        permissions: Permissions,
        config: BlazegraphViewsConfig,
        xas: Transactors
    ) =>
      ValidateBlazegraphView(
        permissions.fetchPermissionSet,
        config.maxViewRefs,
        xas
      )
  }

  make[BlazegraphViews]
    .fromEffect {
      (
          fetchContext: FetchContext,
          contextResolution: ResolverContextResolution,
          validate: ValidateBlazegraphView,
          client: SparqlClient @Id("sparql-indexing-client"),
          config: BlazegraphViewsConfig,
          xas: Transactors,
          clock: Clock[IO],
          uuidF: UUIDF
      ) =>
        BlazegraphViews(
          fetchContext,
          contextResolution,
          validate,
          client,
          config.eventLog,
          config.prefix,
          xas,
          clock
        )(uuidF)
    }

  make[CurrentActiveViews].from { (views: BlazegraphViews) =>
    CurrentActiveViews(views)
  }

  make[SparqlProjectionLifeCycle].from {
    (
        graphStream: GraphResourceStream,
        registry: ReferenceRegistry,
        client: SparqlClient @Id("sparql-indexing-client"),
        config: BlazegraphViewsConfig,
        baseUri: BaseUri
    ) => SparqlProjectionLifeCycle(graphStream, registry, client, config.retryStrategy, config.batch)(baseUri)
  }

  make[SparqlCoordinator].fromEffect {
    (
        views: BlazegraphViews,
        projectionLifeCycle: SparqlProjectionLifeCycle,
        supervisor: Supervisor,
        config: BlazegraphViewsConfig
    ) =>
      SparqlCoordinator(
        views,
        projectionLifeCycle,
        supervisor,
        config.indexingEnabled
      )
  }

  make[BlazegraphViewsQuery].from {
    (
        aclCheck: AclCheck,
        views: BlazegraphViews,
        client: SparqlClient @Id("sparql-query-client"),
        slowQueryLogger: SparqlSlowQueryLogger,
        cfg: BlazegraphViewsConfig,
        xas: Transactors
    ) =>
      BlazegraphViewsQuery(aclCheck, views, client, slowQueryLogger, cfg.prefix, xas)
  }

  make[IncomingOutgoingLinks].fromEffect {
    (
        fetchContext: FetchContext,
        views: BlazegraphViews,
        client: SparqlClient @Id("sparql-query-client"),
        base: BaseUri
    ) =>
      Queries.load.map { queries =>
        IncomingOutgoingLinks(fetchContext, views, client, queries)(base)
      }
  }

  make[SparqlRestartScheduler].from {
    (currentActiveViews: CurrentActiveViews, restartScheduler: ProjectionsRestartScheduler) =>
      SparqlRestartScheduler(currentActiveViews, restartScheduler)
  }

  make[SparqlSupervision].from {
    (currentActiveViews: CurrentActiveViews, client: SparqlClient @Id("sparql-indexing-client")) =>
      SparqlSupervision(client, BlazegraphViewByNamespace(currentActiveViews))
  }

  make[BlazegraphViewsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        views: BlazegraphViews,
        viewsQuery: BlazegraphViewsQuery,
        incomingOutgoingLinks: IncomingOutgoingLinks,
        baseUri: BaseUri,
        cfg: BlazegraphViewsConfig,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig
    ) =>
      new BlazegraphViewsRoutes(
        views,
        viewsQuery,
        incomingOutgoingLinks,
        identities,
        aclCheck
      )(
        baseUri,
        cr,
        ordering,
        cfg.pagination,
        fusionConfig
      )
  }

  make[BlazegraphViewsIndexingRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        views: BlazegraphViews,
        sparqlRestartScheduler: SparqlRestartScheduler,
        projectionDirectives: ProjectionsDirectives,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new BlazegraphViewsIndexingRoutes(
        views.fetchIndexingView(_, _),
        sparqlRestartScheduler,
        identities,
        aclCheck,
        projectionDirectives
      )(cr, ordering)
  }

  make[SparqlSupervisionRoutes].from {
    (
        sparqlSupervision: SparqlSupervision,
        slowQueryLogger: SparqlSlowQueryLogger,
        identities: Identities,
        aclCheck: AclCheck,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new SparqlSupervisionRoutes(sparqlSupervision, slowQueryLogger, identities, aclCheck)(baseUri, cr, ordering)
  }

  make[BlazegraphScopeInitialization].from {
    (views: BlazegraphViews, serviceAccount: ServiceAccount, config: BlazegraphViewsConfig) =>
      new BlazegraphScopeInitialization(views, serviceAccount, config.defaults)
  }
  many[ScopeInitialization].ref[BlazegraphScopeInitialization]

  many[ProjectDeletionTask].add { (currentActiveViews: CurrentActiveViews, views: BlazegraphViews) =>
    BlazegraphDeletionTask(currentActiveViews, views)
  }

  many[ViewsList].add { (views: BlazegraphViews) => ViewsList(views.list) }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/sparql-metadata.json"))

  many[SseEncoder[?]].add { base: BaseUri => BlazegraphViewEvent.sseEncoder(base) }

  many[RemoteContextResolution].addEffect(
    for {
      blazegraphCtx     <- ContextValue.fromFile("contexts/sparql.json")
      blazegraphMetaCtx <- ContextValue.fromFile("contexts/sparql-metadata.json")
    } yield RemoteContextResolution.fixed(
      contexts.blazegraph         -> blazegraphCtx,
      contexts.blazegraphMetadata -> blazegraphMetaCtx
    )
  )

  many[ApiMappings].add(BlazegraphViews.mappings)

  many[PriorityRoute].add {
    (
        bg: BlazegraphViewsRoutes,
        indexing: BlazegraphViewsIndexingRoutes,
        supervision: SparqlSupervisionRoutes,
        schemeDirectives: DeltaSchemeDirectives,
        baseUri: BaseUri
    ) =>
      PriorityRoute(
        priority,
        BlazegraphViewsRoutesHandler(
          schemeDirectives,
          bg.routes,
          indexing.routes,
          supervision.routes
        )(baseUri),
        requiresStrictEntity = true
      )
  }

  many[ServiceDependency].add { (client: SparqlClient @Id("sparql-indexing-client")) =>
    new SparqlServiceDependency(client)
  }

  many[IndexingAction].add {
    (
        currentActiveViews: CurrentActiveViews,
        registry: ReferenceRegistry,
        client: SparqlClient @Id("sparql-indexing-client"),
        config: BlazegraphViewsConfig,
        baseUri: BaseUri
    ) =>
      SparqlIndexingAction(currentActiveViews, registry, client, config.syncIndexingTimeout)(baseUri)
  }
}
