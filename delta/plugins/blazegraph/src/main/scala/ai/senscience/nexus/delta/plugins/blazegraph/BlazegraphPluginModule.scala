package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.config.BlazegraphViewsConfig
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.BlazegraphCoordinator
import ai.senscience.nexus.delta.plugins.blazegraph.model.{contexts, BlazegraphViewEvent}
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks.Queries
import ai.senscience.nexus.delta.plugins.blazegraph.routes.{BlazegraphSupervisionRoutes, BlazegraphViewsIndexingRoutes, BlazegraphViewsRoutes, BlazegraphViewsRoutesHandler}
import ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.{SparqlSlowQueryDeleter, SparqlSlowQueryLogger, SparqlSlowQueryStore}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
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
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
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

  make[SparqlSlowQueryDeleter].fromEffect {
    (supervisor: Supervisor, store: SparqlSlowQueryStore, cfg: BlazegraphViewsConfig, clock: Clock[IO]) =>
      SparqlSlowQueryDeleter.start(
        supervisor,
        store,
        cfg.slowQueries.logTtl,
        cfg.slowQueries.deleteExpiredLogsEvery,
        clock
      )
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

  make[BlazegraphCoordinator].fromEffect {
    (
        views: BlazegraphViews,
        graphStream: GraphResourceStream,
        registry: ReferenceRegistry,
        supervisor: Supervisor,
        client: SparqlClient @Id("sparql-indexing-client"),
        config: BlazegraphViewsConfig,
        baseUri: BaseUri
    ) =>
      BlazegraphCoordinator(
        views,
        graphStream,
        registry,
        supervisor,
        client,
        config
      )(baseUri)
  }

  make[BlazegraphViewsQuery].from {
    (
        aclCheck: AclCheck,
        fetchContext: FetchContext,
        views: BlazegraphViews,
        client: SparqlClient @Id("sparql-query-client"),
        slowQueryLogger: SparqlSlowQueryLogger,
        cfg: BlazegraphViewsConfig,
        xas: Transactors
    ) =>
      BlazegraphViewsQuery(
        aclCheck,
        fetchContext,
        views,
        client,
        slowQueryLogger,
        cfg.prefix,
        xas
      )
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
        projections: Projections,
        projectionErrors: ProjectionErrors,
        baseUri: BaseUri,
        cfg: BlazegraphViewsConfig,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new BlazegraphViewsIndexingRoutes(
        views.fetchIndexingView(_, _),
        identities,
        aclCheck,
        projections,
        projectionErrors
      )(
        baseUri,
        cr,
        ordering,
        cfg.pagination
      )
  }

  make[BlazegraphSupervisionRoutes].from {
    (
        views: BlazegraphViews,
        client: SparqlClient @Id("sparql-indexing-client"),
        identities: Identities,
        aclCheck: AclCheck,
        ordering: JsonKeyOrdering
    ) =>
      BlazegraphSupervisionRoutes(views, client, identities, aclCheck)(ordering)
  }

  make[BlazegraphScopeInitialization].from {
    (views: BlazegraphViews, serviceAccount: ServiceAccount, config: BlazegraphViewsConfig) =>
      new BlazegraphScopeInitialization(views, serviceAccount, config.defaults)
  }
  many[ScopeInitialization].ref[BlazegraphScopeInitialization]

  many[ProjectDeletionTask].add { (views: BlazegraphViews) => BlazegraphDeletionTask(views) }

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
        supervision: BlazegraphSupervisionRoutes,
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
        views: BlazegraphViews,
        registry: ReferenceRegistry,
        client: SparqlClient @Id("sparql-indexing-client"),
        config: BlazegraphViewsConfig,
        baseUri: BaseUri
    ) =>
      SparqlIndexingAction(views, registry, client, config.syncIndexingTimeout)(baseUri)
  }
}
