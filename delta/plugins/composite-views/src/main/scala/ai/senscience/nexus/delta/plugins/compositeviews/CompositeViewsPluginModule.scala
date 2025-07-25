package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.compositeviews.client.DeltaClient
import ai.senscience.nexus.delta.plugins.compositeviews.config.CompositeViewsConfig
import ai.senscience.nexus.delta.plugins.compositeviews.deletion.CompositeViewsDeletionTask
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.*
import ai.senscience.nexus.delta.plugins.compositeviews.model.{contexts, CompositeViewEvent}
import ai.senscience.nexus.delta.plugins.compositeviews.projections.{CompositeIndexingDetails, CompositeProjections}
import ai.senscience.nexus.delta.plugins.compositeviews.routes.{CompositeSupervisionRoutes, CompositeViewsIndexingRoutes, CompositeViewsRoutes, CompositeViewsRoutesHandler}
import ai.senscience.nexus.delta.plugins.compositeviews.store.CompositeRestartStore
import ai.senscience.nexus.delta.plugins.compositeviews.stream.{CompositeGraphStream, RemoteGraphStream}
import ai.senscience.nexus.delta.rdf.Triple
import ai.senscience.nexus.delta.rdf.jsonld.api.JsonLdOptions
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, JsonLdContext, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.auth.AuthTokenProvider
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.projects.{FetchContext, Projects}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.ViewsList
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.projections.ProjectionErrors
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import ai.senscience.nexus.delta.sourcing.stream.config.ProjectionConfig
import ai.senscience.nexus.delta.sourcing.stream.{PipeChain, ReferenceRegistry, Supervisor}
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id

class CompositeViewsPluginModule(priority: Int) extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[CompositeViewsConfig]("plugins.composite-views")

  make[DeltaClient].fromResource { (cfg: CompositeViewsConfig, authTokenProvider: AuthTokenProvider) =>
    DeltaClient(authTokenProvider, cfg.remoteSourceCredentials, cfg.remoteSourceClient.retryDelay)
  }

  make[SparqlClient].named("sparql-composite-indexing-client").fromResource { (cfg: CompositeViewsConfig) =>
    val access = cfg.blazegraphAccess
    SparqlClient(access.sparqlTarget, access.base, access.queryTimeout, cfg.blazegraphAccess.credentials)
  }

  make[SparqlClient].named("sparql-composite-query-client").fromResource { (cfg: CompositeViewsConfig) =>
    val access = cfg.blazegraphAccess
    SparqlClient(access.sparqlTarget, access.base, access.queryTimeout, cfg.blazegraphAccess.credentials)
  }

  make[ValidateCompositeView].from {
    (
        aclCheck: AclCheck,
        projects: Projects,
        permissions: Permissions,
        client: ElasticSearchClient,
        deltaClient: DeltaClient,
        config: CompositeViewsConfig,
        baseUri: BaseUri
    ) =>
      ValidateCompositeView(
        aclCheck,
        projects,
        permissions.fetchPermissionSet,
        client,
        deltaClient,
        config.prefix,
        config.sources.maxSources,
        config.maxProjections
      )(baseUri)
  }

  make[CompositeViews].fromEffect {
    (
        fetchContext: FetchContext,
        contextResolution: ResolverContextResolution,
        validate: ValidateCompositeView,
        config: CompositeViewsConfig,
        xas: Transactors,
        uuidF: UUIDF,
        clock: Clock[IO]
    ) =>
      CompositeViews(
        fetchContext,
        contextResolution,
        validate,
        config.minIntervalRebuild,
        config.eventLog,
        xas,
        clock
      )(uuidF)
  }

  make[CompositeRestartStore].from { (xas: Transactors) =>
    new CompositeRestartStore(xas)
  }

  make[CompositeProjections].from {
    (
        compositeRestartStore: CompositeRestartStore,
        config: CompositeViewsConfig,
        projectionConfig: ProjectionConfig,
        clock: Clock[IO],
        xas: Transactors
    ) =>
      CompositeProjections(
        compositeRestartStore,
        xas,
        projectionConfig.query,
        projectionConfig.batch,
        config.restartCheckInterval,
        clock
      )
  }

  many[PurgeProjection].add { (compositeRestartStore: CompositeRestartStore, projectionConfig: ProjectionConfig) =>
    CompositeRestartStore.purgeExpiredRestarts(compositeRestartStore, projectionConfig.restartPurge)
  }

  make[CompositeSpaces].from {
    (
        esClient: ElasticSearchClient,
        sparqlClient: SparqlClient @Id("sparql-composite-indexing-client"),
        cfg: CompositeViewsConfig
    ) =>
      CompositeSpaces(cfg.prefix, esClient, sparqlClient)
  }

  make[CompositeSinks].from {
    (
        esClient: ElasticSearchClient,
        sparqlClient: SparqlClient @Id("sparql-composite-indexing-client"),
        cfg: CompositeViewsConfig,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate")
    ) =>
      CompositeSinks(
        cfg.prefix,
        esClient,
        cfg.elasticsearchBatch,
        sparqlClient,
        cfg.blazegraphBatch,
        cfg.sinkConfig,
        cfg.retryStrategy
      )(baseUri, cr)
  }

  make[MetadataPredicates].fromEffect {
    (
        listingsMetadataCtx: MetadataContextValue @Id("search-metadata"),
        cr: RemoteContextResolution @Id("aggregate")
    ) =>
      JsonLdContext(listingsMetadataCtx.value)(cr, JsonLdOptions.defaults)
        .map(_.aliasesInv.keySet.map(Triple.predicate))
        .map(MetadataPredicates)
  }

  make[RemoteGraphStream].from {
    (
        deltaClient: DeltaClient,
        config: CompositeViewsConfig,
        metadataPredicates: MetadataPredicates
    ) =>
      new RemoteGraphStream(deltaClient, config.remoteSourceClient, metadataPredicates)
  }

  make[CompositeGraphStream].from { (local: GraphResourceStream, remote: RemoteGraphStream) =>
    CompositeGraphStream(local, remote)
  }

  many[CompositeProjectionLifeCycle.Hook].addValue(CompositeProjectionLifeCycle.NoopHook)

  make[CompositeProjectionLifeCycle].from {
    (
        hooks: Set[CompositeProjectionLifeCycle.Hook],
        registry: ReferenceRegistry,
        graphStream: CompositeGraphStream,
        spaces: CompositeSpaces,
        sinks: CompositeSinks,
        compositeProjections: CompositeProjections
    ) =>
      CompositeProjectionLifeCycle(
        hooks,
        PipeChain.compile(_, registry),
        graphStream,
        spaces,
        sinks,
        compositeProjections
      )
  }

  make[CompositeViewsCoordinator].fromEffect {
    (
        compositeViews: CompositeViews,
        supervisor: Supervisor,
        lifecycle: CompositeProjectionLifeCycle,
        config: CompositeViewsConfig
    ) =>
      CompositeViewsCoordinator(
        compositeViews,
        supervisor,
        lifecycle,
        config
      )
  }

  many[ProjectDeletionTask].add { (views: CompositeViews) => CompositeViewsDeletionTask(views) }

  many[ViewsList].add { (views: CompositeViews) => ViewsList(views.list) }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/composite-views-metadata.json"))

  many[RemoteContextResolution].addEffect(
    for {
      ctx     <- ContextValue.fromFile("contexts/composite-views.json")
      metaCtx <- ContextValue.fromFile("contexts/composite-views-metadata.json")
    } yield RemoteContextResolution.fixed(
      contexts.compositeViews         -> ctx,
      contexts.compositeViewsMetadata -> metaCtx
    )
  )

  make[BlazegraphQuery].from {
    (
        aclCheck: AclCheck,
        views: CompositeViews,
        client: SparqlClient @Id("sparql-composite-query-client"),
        cfg: CompositeViewsConfig
    ) => BlazegraphQuery(aclCheck, views, client, cfg.prefix)
  }

  make[ElasticSearchQuery].from {
    (aclCheck: AclCheck, views: CompositeViews, client: ElasticSearchClient, cfg: CompositeViewsConfig) =>
      ElasticSearchQuery(aclCheck, views, client, cfg.prefix)
  }

  make[CompositeViewsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        views: CompositeViews,
        blazegraphQuery: BlazegraphQuery,
        elasticSearchQuery: ElasticSearchQuery,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig
    ) =>
      new CompositeViewsRoutes(
        identities,
        aclCheck,
        views,
        blazegraphQuery,
        elasticSearchQuery
      )(baseUri, cr, ordering, fusionConfig)
  }

  make[CompositeViewsIndexingRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        views: CompositeViews,
        graphStream: CompositeGraphStream,
        projections: CompositeProjections,
        projectionErrors: ProjectionErrors,
        baseUri: BaseUri,
        config: CompositeViewsConfig,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new CompositeViewsIndexingRoutes(
        identities,
        aclCheck,
        views.fetchIndexingView,
        views.expand,
        CompositeIndexingDetails(projections, graphStream, config.prefix),
        projections,
        projectionErrors
      )(baseUri, config.pagination, cr, ordering)
  }

  make[CompositeSupervisionRoutes].from {
    (
        views: CompositeViews,
        client: SparqlClient @Id("sparql-composite-indexing-client"),
        identities: Identities,
        aclCheck: AclCheck,
        config: CompositeViewsConfig,
        ordering: JsonKeyOrdering
    ) =>
      CompositeSupervisionRoutes(views, client, identities, aclCheck, config.prefix)(ordering)
  }

  many[SseEncoder[?]].add { (base: BaseUri) => CompositeViewEvent.sseEncoder(base) }

  many[PriorityRoute].add {
    (
        cv: CompositeViewsRoutes,
        indexing: CompositeViewsIndexingRoutes,
        supervision: CompositeSupervisionRoutes,
        schemeDirectives: DeltaSchemeDirectives,
        baseUri: BaseUri
    ) =>
      PriorityRoute(
        priority,
        CompositeViewsRoutesHandler(
          schemeDirectives,
          cv.routes,
          indexing.routes,
          supervision.routes
        )(baseUri),
        requiresStrictEntity = true
      )
  }
}
