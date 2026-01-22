package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.elasticsearch.config.{ElasticSearchViewsConfig, MainIndexConfig}
import ai.senscience.nexus.delta.elasticsearch.configured.ConfiguredIndexingConfig
import ai.senscience.nexus.delta.elasticsearch.deletion.{ConfiguredIndexDeletionTask, ElasticSearchDeletionTask, EventMetricsDeletionTask, MainIndexDeletionTask}
import ai.senscience.nexus.delta.elasticsearch.indexing.*
import ai.senscience.nexus.delta.elasticsearch.main.MainIndexDef
import ai.senscience.nexus.delta.elasticsearch.metrics.{EventMetrics, EventMetricsProjection, MetricsIndexDef}
import ai.senscience.nexus.delta.elasticsearch.model.{contexts, ElasticSearchViewEvent}
import ai.senscience.nexus.delta.elasticsearch.query.{ConfiguredIndexQuery, MainIndexQuery}
import ai.senscience.nexus.delta.elasticsearch.routes.*
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.directives.{DeltaSchemeDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.indexing.{MainDocumentEncoder, ProjectProjectionFactory, SyncIndexingAction}
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.metrics.ScopedEventMetricEncoder
import ai.senscience.nexus.delta.sdk.otel.OtelMetricsClient
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.projects.{FetchContext, ProjectScopeResolver, Projects}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.stream.{AnnotatedSourceStream, GraphResourceStream, MainDocumentStream}
import ai.senscience.nexus.delta.sdk.views.ViewsList
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.projections.{Projections, ProjectionsRestartScheduler}
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming
import ai.senscience.nexus.delta.sourcing.stream.{PipeChainCompiler, Supervisor}
import cats.effect.{Clock, IO}
import com.typesafe.config.Config
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * ElasticSearch plugin wiring.
  */
class ElasticSearchModule(pluginsMinPriority: Int) extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[ElasticSearchViewsConfig]("app.elasticsearch")

  makeTracer("elasticsearch")

  makeTracer("elasticsearch-indexing")

  make[MetricsIndexDef].fromEffect { (cfg: ElasticSearchViewsConfig) =>
    MetricsIndexDef(cfg.prefix)
  }

  make[MainIndexConfig].from { (cfg: ElasticSearchViewsConfig) => cfg.mainIndex }

  make[DefaultIndexDef].fromEffect { DefaultIndexDef.load() }

  make[MainIndexDef].fromEffect { (cfg: MainIndexConfig) => MainIndexDef(cfg) }

  private def buildElasticsearchClient(
      cfg: ElasticSearchViewsConfig,
      metricsClient: OtelMetricsClient,
      traffic: String
  )(using tracer: Tracer[IO]) =
    ElasticSearchClient(
      cfg.base,
      cfg.credentials,
      cfg.maxIndexPathLength,
      metricsClient,
      traffic,
      cfg.otel
    )

  make[ElasticSearchClient].named("elasticsearch-indexing-client").fromResource {
    (
        cfg: ElasticSearchViewsConfig,
        metricsClient: OtelMetricsClient,
        tracer: Tracer[IO] @Id("elasticsearch-indexing")
    ) =>
      buildElasticsearchClient(cfg, metricsClient, "elasticsearch-indexing")(using tracer)
  }

  make[ElasticSearchClient].named("elasticsearch-query-client").fromResource {
    (cfg: ElasticSearchViewsConfig, metricsClient: OtelMetricsClient, tracer: Tracer[IO] @Id("elasticsearch")) =>
      buildElasticsearchClient(cfg, metricsClient, "elasticsearch-query")(using tracer)
  }

  make[ValidateElasticSearchView].from {
    (
        pipeChainCompiler: PipeChainCompiler,
        permissions: Permissions,
        client: ElasticSearchClient @Id("elasticsearch-indexing-client"),
        config: ElasticSearchViewsConfig,
        defaultIndex: DefaultIndexDef,
        xas: Transactors
    ) =>
      ValidateElasticSearchView(
        pipeChainCompiler,
        permissions,
        client,
        config.prefix,
        config.maxViewRefs,
        xas,
        defaultIndex
      )
  }

  make[ElasticSearchViews].fromEffect {
    (
        fetchContext: FetchContext,
        contextResolution: ResolverContextResolution,
        validateElasticSearchView: ValidateElasticSearchView,
        config: ElasticSearchViewsConfig,
        defaultIndex: DefaultIndexDef,
        xas: Transactors,
        clock: Clock[IO],
        uuidF: UUIDF,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      ElasticSearchViews(
        fetchContext,
        contextResolution,
        validateElasticSearchView,
        config.eventLog,
        config.prefix,
        xas,
        defaultIndex,
        clock
      )(using uuidF)(using tracer)
  }

  make[CurrentActiveViews].from { (views: ElasticSearchViews) =>
    CurrentActiveViews(views)
  }

  make[ElasticsearchRestartScheduler].from {
    (currentActiveViews: CurrentActiveViews, restartScheduler: ProjectionsRestartScheduler) =>
      ElasticsearchRestartScheduler(currentActiveViews, restartScheduler)
  }

  make[ElasticSearchCoordinator].fromEffect {
    (
        views: ElasticSearchViews,
        graphStream: GraphResourceStream,
        pipeChainCompiler: PipeChainCompiler,
        supervisor: Supervisor,
        client: ElasticSearchClient @Id("elasticsearch-indexing-client"),
        config: ElasticSearchViewsConfig,
        cr: RemoteContextResolution @Id("aggregate"),
        tracer: Tracer[IO] @Id("elasticsearch-indexing")
    ) =>
      ElasticSearchCoordinator(
        views,
        graphStream,
        pipeChainCompiler,
        supervisor,
        client,
        config
      )(using cr, tracer)
  }

  make[MainRestartScheduler].from { (projects: Projects, restartScheduler: ProjectionsRestartScheduler) =>
    MainRestartScheduler(projects, restartScheduler)
  }

  make[MainDocumentEncoder.Aggregate].from { (mainDocumentEncoders: Set[MainDocumentEncoder[?, ?]]) =>
    MainDocumentEncoder.Aggregate(mainDocumentEncoders)
  }

  make[MainDocumentStream].from { (elemStreaming: ElemStreaming, mainDocumentEncoder: MainDocumentEncoder.Aggregate) =>
    MainDocumentStream(elemStreaming, mainDocumentEncoder)
  }

  many[ProjectProjectionFactory].addSet {
    (
        mainDocumentStream: MainDocumentStream,
        client: ElasticSearchClient @Id("elasticsearch-indexing-client"),
        mainIndex: MainIndexDef,
        config: ElasticSearchViewsConfig,
        tracer: Tracer[IO] @Id("elasticsearch-indexing")
    ) =>
      MainIndexingProjectionFactory(mainDocumentStream, client, mainIndex, config.batch, config.indexingEnabled)(using
        tracer
      ).toSet
  }

  make[ConfiguredIndexingConfig].fromEffect { (config: Config) =>
    ConfiguredIndexingConfig.load(config)
  }

  many[ProjectProjectionFactory].addSet {
    (
        annotatedSourceStream: AnnotatedSourceStream,
        client: ElasticSearchClient @Id("elasticsearch-indexing-client"),
        configuredConfig: ConfiguredIndexingConfig,
        config: ElasticSearchViewsConfig,
        baseUri: BaseUri,
        tracer: Tracer[IO] @Id("elasticsearch-indexing")
    ) =>
      ConfiguredIndexingProjectionFactory(
        annotatedSourceStream,
        client,
        configuredConfig,
        config.batch,
        config.indexingEnabled
      )(using baseUri, tracer).toSet
  }

  make[EventMetricsProjection].fromEffect {
    (
        metricEncoders: Set[ScopedEventMetricEncoder[?]],
        xas: Transactors,
        supervisor: Supervisor,
        projections: Projections,
        eventMetrics: EventMetrics,
        config: ElasticSearchViewsConfig
    ) =>
      EventMetricsProjection(
        metricEncoders,
        supervisor,
        projections,
        eventMetrics,
        xas,
        config.batch,
        config.metricsQuery,
        config.indexingEnabled
      )
  }

  make[ElasticSearchViewsQuery].from {
    (
        aclCheck: AclCheck,
        views: ElasticSearchViews,
        client: ElasticSearchClient @Id("elasticsearch-query-client"),
        xas: Transactors,
        cfg: ElasticSearchViewsConfig,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      ElasticSearchViewsQuery(
        aclCheck,
        views,
        client,
        cfg.prefix,
        xas
      )(using tracer)
  }

  make[MainIndexQuery].from {
    (
        client: ElasticSearchClient @Id("elasticsearch-query-client"),
        baseUri: BaseUri,
        config: MainIndexConfig,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) => MainIndexQuery(client, config)(using baseUri, tracer)
  }

  make[ConfiguredIndexQuery].fromEffect {
    (
        client: ElasticSearchClient @Id("elasticsearch-query-client"),
        config: ConfiguredIndexingConfig,
        clock: Clock[IO],
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      ConfiguredIndexQuery(client, config, clock)(using tracer)
  }

  make[ElasticSearchViewsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        views: ElasticSearchViews,
        viewsQuery: ElasticSearchViewsQuery,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      new ElasticSearchViewsRoutes(
        identities,
        aclCheck,
        views,
        viewsQuery
      )(using
        baseUri,
        cr,
        ordering,
        fusionConfig,
        tracer
      )
  }

  make[MainIndexRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        mainIndexQuery: MainIndexQuery,
        restartScheduler: MainRestartScheduler,
        projectionsDirectives: ProjectionsDirectives,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      new MainIndexRoutes(identities, aclCheck, mainIndexQuery, restartScheduler, projectionsDirectives)(using
        cr,
        ordering,
        tracer
      )
  }

  make[ConfiguredIndexRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        configuredQuery: ConfiguredIndexQuery,
        projectionDirectives: ProjectionsDirectives,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      new ConfiguredIndexRoutes(identities, aclCheck, configuredQuery, projectionDirectives)(using cr, ordering, tracer)
  }

  make[ListingRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        projectScopeResolver: ProjectScopeResolver,
        schemeDirectives: DeltaSchemeDirectives,
        mainIndexQuery: MainIndexQuery,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        resourcesToSchemaSet: Set[ResourceToSchemaMappings],
        esConfig: ElasticSearchViewsConfig,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      val resourceToSchema = resourcesToSchemaSet.foldLeft(ResourceToSchemaMappings.empty)(_ + _)
      new ListingRoutes(
        identities,
        aclCheck,
        projectScopeResolver,
        resourceToSchema,
        schemeDirectives,
        mainIndexQuery
      )(using baseUri, esConfig.pagination, cr, ordering, tracer)
  }

  make[ElasticSearchIndexingRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        views: ElasticSearchViews,
        restartScheduler: ElasticsearchRestartScheduler,
        client: ElasticSearchClient @Id("elasticsearch-query-client"),
        projectionsDirectives: ProjectionsDirectives,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      ElasticSearchIndexingRoutes(
        identities,
        aclCheck,
        views.fetchIndexingView(_, _),
        restartScheduler,
        projectionsDirectives,
        client
      )(using cr, ordering, tracer)
  }

  make[IdResolution].from {
    (projectScopeResolver: ProjectScopeResolver, defaultIndexQuery: MainIndexQuery, shifts: ResourceShifts) =>
      IdResolution(
        projectScopeResolver,
        defaultIndexQuery,
        (resourceRef, projectRef) => shifts.fetch(resourceRef, projectRef)
      )
  }

  make[IdResolutionRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        idResolution: IdResolution,
        ordering: JsonKeyOrdering,
        rcr: RemoteContextResolution @Id("aggregate"),
        fusionConfig: FusionConfig,
        baseUri: BaseUri,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      new IdResolutionRoutes(identities, aclCheck, idResolution)(using baseUri, fusionConfig)(using
        ordering,
        rcr,
        tracer
      )
  }

  make[EventMetrics].from {
    (client: ElasticSearchClient @Id("elasticsearch-indexing-client"), metricsIndex: MetricsIndexDef) =>
      EventMetrics(client, metricsIndex)
  }

  make[ElasticSearchHistoryRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        eventMetrics: EventMetrics,
        rcr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("elasticsearch")
    ) =>
      new ElasticSearchHistoryRoutes(identities, aclCheck, eventMetrics)(using rcr, ordering, tracer)
  }

  many[ProjectDeletionTask].add { (currentViews: CurrentActiveViews, views: ElasticSearchViews) =>
    ElasticSearchDeletionTask(currentViews, views)
  }

  many[ProjectDeletionTask].add { (eventMetrics: EventMetrics) => new EventMetricsDeletionTask(eventMetrics) }

  many[ProjectDeletionTask].add {
    (client: ElasticSearchClient @Id("elasticsearch-indexing-client"), config: MainIndexConfig) =>
      new MainIndexDeletionTask(client, config.index)
  }

  many[ProjectDeletionTask].addSet {
    (client: ElasticSearchClient @Id("elasticsearch-indexing-client"), config: ConfiguredIndexingConfig) =>
      ConfiguredIndexDeletionTask(client, config).toSet
  }

  many[ViewsList].add { (views: ElasticSearchViews) =>
    ViewsList(views.list)
  }

  make[MetadataContextValue]
    .named("search-metadata")
    .from((agg: Set[MetadataContextValue]) => agg.foldLeft(MetadataContextValue.empty)(_.merge(_)))

  make[MetadataContextValue]
    .named("indexing-metadata")
    .from { (listingsMetadataCtx: MetadataContextValue @Id("search-metadata")) =>
      MetadataContextValue(listingsMetadataCtx.value.visit(obj = { case ContextObject(obj) =>
        ContextObject(obj.filterKeys(_.startsWith("_")))
      }))
    }

  many[SseEncoder[?]].add { (base: BaseUri) => ElasticSearchViewEvent.sseEncoder(base) }

  many[RemoteContextResolution].addEffect {
    (
        searchMetadataCtx: MetadataContextValue @Id("search-metadata"),
        indexingMetadataCtx: MetadataContextValue @Id("indexing-metadata")
    ) =>
      RemoteContextResolution.loadResources(contexts.definition).map {
        _.merge(
          RemoteContextResolution.fixed(
            contexts.indexingMetadata -> indexingMetadataCtx.value,
            contexts.searchMetadata   -> searchMetadataCtx.value
          )
        )
      }
  }

  many[ApiMappings].add(ElasticSearchViews.mappings)

  many[PriorityRoute].add {
    (
        es: ElasticSearchViewsRoutes,
        query: ListingRoutes,
        indexing: ElasticSearchIndexingRoutes,
        defaultIndex: MainIndexRoutes,
        configuredIndex: ConfiguredIndexRoutes,
        idResolutionRoute: IdResolutionRoutes,
        historyRoutes: ElasticSearchHistoryRoutes,
        schemeDirectives: DeltaSchemeDirectives,
        baseUri: BaseUri
    ) =>
      PriorityRoute(
        pluginsMinPriority - 1,
        ElasticSearchViewsRoutesHandler(
          schemeDirectives,
          es.routes,
          query.routes,
          indexing.routes,
          defaultIndex.routes,
          configuredIndex.routes,
          idResolutionRoute.routes,
          historyRoutes.routes
        )(using baseUri),
        requiresStrictEntity = true
      )
  }

  many[ServiceDependency].add { (client: ElasticSearchClient @Id("elasticsearch-indexing-client")) =>
    new ElasticSearchServiceDependency(client)
  }

  many[SyncIndexingAction].addSet {
    (
        resourceShifts: ResourceShifts,
        mainDocumentEncoder: MainDocumentEncoder.Aggregate,
        currentViews: CurrentActiveViews,
        pipeChainCompiler: PipeChainCompiler,
        client: ElasticSearchClient @Id("elasticsearch-indexing-client"),
        config: ElasticSearchViewsConfig,
        cr: RemoteContextResolution @Id("aggregate"),
        tracer: Tracer[IO] @Id("elasticsearch-indexing")
    ) =>
      Set(
        ElasticSearchIndexingAction(
          resourceShifts,
          currentViews,
          pipeChainCompiler,
          client,
          config.syncIndexingTimeout,
          config.syncIndexingRefresh
        )(using cr, tracer),
        MainIndexingAction(
          mainDocumentEncoder,
          client,
          config.mainIndex,
          config.syncIndexingTimeout,
          config.syncIndexingRefresh
        )(using tracer)
      )

  }
}
