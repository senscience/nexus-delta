package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.elasticsearch.config.ElasticSearchViewsConfig
import ai.senscience.nexus.delta.elasticsearch.deletion.{ElasticSearchDeletionTask, EventMetricsDeletionTask, MainIndexDeletionTask}
import ai.senscience.nexus.delta.elasticsearch.indexing.{ElasticSearchCoordinator, MainIndexingAction, MainIndexingCoordinator}
import ai.senscience.nexus.delta.elasticsearch.main.MainIndexDef
import ai.senscience.nexus.delta.elasticsearch.metrics.{EventMetrics, EventMetricsProjection, MetricsIndexDef}
import ai.senscience.nexus.delta.elasticsearch.model.{contexts, ElasticSearchViewEvent}
import ai.senscience.nexus.delta.elasticsearch.query.MainIndexQuery
import ai.senscience.nexus.delta.elasticsearch.routes.*
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.directives.{DeltaSchemeDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.indexing.IndexingAction
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.metrics.ScopedEventMetricEncoder
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.projects.{FetchContext, ProjectScopeResolver, Projects}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.ViewsList
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.projections.Projections
import ai.senscience.nexus.delta.sourcing.stream.{PipeChain, ReferenceRegistry, Supervisor}
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id

/**
  * ElasticSearch plugin wiring.
  */
class ElasticSearchModule(pluginsMinPriority: Int) extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[ElasticSearchViewsConfig]("app.elasticsearch")

  make[MetricsIndexDef].fromEffect { (cfg: ElasticSearchViewsConfig) =>
    MetricsIndexDef(cfg.prefix, loader)
  }

  make[DefaultIndexDef].fromEffect { DefaultIndexDef(loader) }

  make[MainIndexDef].fromEffect { (cfg: ElasticSearchViewsConfig) =>
    MainIndexDef(cfg.mainIndex, loader)
  }

  make[ElasticSearchClient].fromResource { (cfg: ElasticSearchViewsConfig) =>
    ElasticSearchClient(cfg.base, cfg.credentials, cfg.maxIndexPathLength)
  }

  make[ValidateElasticSearchView].from {
    (
        registry: ReferenceRegistry,
        permissions: Permissions,
        client: ElasticSearchClient,
        config: ElasticSearchViewsConfig,
        defaultIndex: DefaultIndexDef,
        xas: Transactors
    ) =>
      ValidateElasticSearchView(
        PipeChain.validate(_, registry),
        permissions,
        client: ElasticSearchClient,
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
        uuidF: UUIDF
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
      )(uuidF)
  }

  make[ElasticSearchCoordinator].fromEffect {
    (
        views: ElasticSearchViews,
        graphStream: GraphResourceStream,
        registry: ReferenceRegistry,
        supervisor: Supervisor,
        client: ElasticSearchClient,
        config: ElasticSearchViewsConfig,
        cr: RemoteContextResolution @Id("aggregate")
    ) =>
      ElasticSearchCoordinator(
        views,
        graphStream,
        registry,
        supervisor,
        client,
        config
      )(cr)
  }

  make[MainIndexingCoordinator].fromEffect {
    (
        projects: Projects,
        graphStream: GraphResourceStream,
        supervisor: Supervisor,
        client: ElasticSearchClient,
        mainIndex: MainIndexDef,
        config: ElasticSearchViewsConfig,
        cr: RemoteContextResolution @Id("aggregate")
    ) =>
      MainIndexingCoordinator(
        projects,
        graphStream,
        supervisor,
        client,
        mainIndex,
        config.batch,
        config.indexingEnabled
      )(cr)
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
        client: ElasticSearchClient,
        xas: Transactors,
        cfg: ElasticSearchViewsConfig
    ) =>
      ElasticSearchViewsQuery(
        aclCheck,
        views,
        client,
        cfg.prefix,
        xas
      )
  }

  make[MainIndexQuery].from {
    (
        client: ElasticSearchClient,
        baseUri: BaseUri,
        config: ElasticSearchViewsConfig
    ) => MainIndexQuery(client, config.mainIndex)(baseUri)
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
        fusionConfig: FusionConfig
    ) =>
      new ElasticSearchViewsRoutes(
        identities,
        aclCheck,
        views,
        viewsQuery
      )(
        baseUri,
        cr,
        ordering,
        fusionConfig
      )
  }

  make[MainIndexRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        defaultIndexQuery: MainIndexQuery,
        projections: Projections,
        projectionsDirectives: ProjectionsDirectives,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new MainIndexRoutes(identities, aclCheck, defaultIndexQuery, projections, projectionsDirectives)(cr, ordering)
  }

  make[ListingRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        projectScopeResolver: ProjectScopeResolver,
        schemeDirectives: DeltaSchemeDirectives,
        defaultIndexQuery: MainIndexQuery,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        resourcesToSchemaSet: Set[ResourceToSchemaMappings],
        esConfig: ElasticSearchViewsConfig
    ) =>
      val resourceToSchema = resourcesToSchemaSet.foldLeft(ResourceToSchemaMappings.empty)(_ + _)
      new ListingRoutes(
        identities,
        aclCheck,
        projectScopeResolver,
        resourceToSchema,
        schemeDirectives,
        defaultIndexQuery
      )(baseUri, esConfig.pagination, cr, ordering)
  }

  make[ElasticSearchIndexingRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        views: ElasticSearchViews,
        projections: Projections,
        projectionsDirectives: ProjectionsDirectives,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        viewsQuery: ElasticSearchViewsQuery
    ) =>
      new ElasticSearchIndexingRoutes(
        identities,
        aclCheck,
        views.fetchIndexingView(_, _),
        projections,
        projectionsDirectives,
        viewsQuery
      )(
        cr,
        ordering
      )
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
        baseUri: BaseUri
    ) =>
      new IdResolutionRoutes(identities, aclCheck, idResolution)(
        baseUri,
        ordering,
        rcr,
        fusionConfig
      )
  }

  make[EventMetrics].from { (client: ElasticSearchClient, metricsIndex: MetricsIndexDef) =>
    EventMetrics(client, metricsIndex)
  }

  make[ElasticSearchHistoryRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        eventMetrics: EventMetrics,
        rcr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new ElasticSearchHistoryRoutes(identities, aclCheck, eventMetrics)(rcr, ordering)
  }

  many[ProjectDeletionTask].add { (views: ElasticSearchViews) => ElasticSearchDeletionTask(views) }

  many[ProjectDeletionTask].add { (eventMetrics: EventMetrics) => new EventMetricsDeletionTask(eventMetrics) }

  many[ProjectDeletionTask].add { (client: ElasticSearchClient, config: ElasticSearchViewsConfig) =>
    new MainIndexDeletionTask(client, config.mainIndex.index)
  }

  many[ViewsList].add { (views: ElasticSearchViews) =>
    ViewsList(views.list)
  }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/elasticsearch-metadata.json"))

  make[MetadataContextValue]
    .named("search-metadata")
    .from((agg: Set[MetadataContextValue]) => agg.foldLeft(MetadataContextValue.empty)(_ merge _))

  make[MetadataContextValue]
    .named("indexing-metadata")
    .from { (listingsMetadataCtx: MetadataContextValue @Id("search-metadata")) =>
      MetadataContextValue(listingsMetadataCtx.value.visit(obj = { case ContextObject(obj) =>
        ContextObject(obj.filterKeys(_.startsWith("_")))
      }))
    }

  many[SseEncoder[?]].add { base: BaseUri => ElasticSearchViewEvent.sseEncoder(base) }

  many[RemoteContextResolution].addEffect {
    (
        searchMetadataCtx: MetadataContextValue @Id("search-metadata"),
        indexingMetadataCtx: MetadataContextValue @Id("indexing-metadata")
    ) =>
      for {
        elasticsearchCtx     <- ContextValue.fromFile("contexts/elasticsearch.json")
        elasticsearchMetaCtx <- ContextValue.fromFile("contexts/elasticsearch-metadata.json")
        elasticsearchIdxCtx  <- ContextValue.fromFile("contexts/elasticsearch-indexing.json")
        offsetCtx            <- ContextValue.fromFile("contexts/offset.json")
        statisticsCtx        <- ContextValue.fromFile("contexts/statistics.json")
      } yield RemoteContextResolution.fixed(
        contexts.elasticsearch         -> elasticsearchCtx,
        contexts.elasticsearchMetadata -> elasticsearchMetaCtx,
        contexts.elasticsearchIndexing -> elasticsearchIdxCtx,
        contexts.indexingMetadata      -> indexingMetadataCtx.value,
        contexts.searchMetadata        -> searchMetadataCtx.value,
        Vocabulary.contexts.offset     -> offsetCtx,
        Vocabulary.contexts.statistics -> statisticsCtx
      )
  }

  many[ApiMappings].add(ElasticSearchViews.mappings)

  many[PriorityRoute].add {
    (
        es: ElasticSearchViewsRoutes,
        query: ListingRoutes,
        defaultIndex: MainIndexRoutes,
        indexing: ElasticSearchIndexingRoutes,
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
          defaultIndex.routes,
          indexing.routes,
          idResolutionRoute.routes,
          historyRoutes.routes
        )(baseUri),
        requiresStrictEntity = true
      )
  }

  many[ServiceDependency].add { new ElasticSearchServiceDependency(_) }

  many[IndexingAction].add {
    (
        views: ElasticSearchViews,
        registry: ReferenceRegistry,
        client: ElasticSearchClient,
        config: ElasticSearchViewsConfig,
        cr: RemoteContextResolution @Id("aggregate")
    ) =>
      ElasticSearchIndexingAction(views, registry, client, config.syncIndexingTimeout, config.syncIndexingRefresh)(
        cr
      )
  }

  many[IndexingAction].add {
    (
        client: ElasticSearchClient,
        config: ElasticSearchViewsConfig,
        cr: RemoteContextResolution @Id("aggregate")
    ) => MainIndexingAction(client, config.mainIndex, config.syncIndexingTimeout, config.syncIndexingRefresh)(cr)
  }

}
