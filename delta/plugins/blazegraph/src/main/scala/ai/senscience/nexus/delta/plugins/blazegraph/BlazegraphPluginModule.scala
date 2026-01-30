package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.config.BlazegraphViewsConfig
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.{CurrentActiveViews, SparqlCoordinator, SparqlHealthCheck, SparqlProjectionLifeCycle, SparqlRestartScheduler}
import ai.senscience.nexus.delta.plugins.blazegraph.model.{contexts, BlazegraphViewEvent}
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks.Queries
import ai.senscience.nexus.delta.plugins.blazegraph.routes.{BlazegraphViewsIndexingRoutes, BlazegraphViewsRoutes, BlazegraphViewsRoutesHandler, SparqlSupervisionRoutes}
import ai.senscience.nexus.delta.plugins.blazegraph.supervision.{BlazegraphViewByNamespace, SparqlSupervision}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.directives.{DeltaSchemeDirectives, ProjectionsDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.indexing.SyncIndexingAction
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.otel.OtelMetricsClient
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
import ai.senscience.nexus.delta.sourcing.stream.{PipeChainCompiler, Supervisor}
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Blazegraph plugin wiring
  */
class BlazegraphPluginModule(priority: Int) extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[BlazegraphViewsConfig]("plugins.blazegraph")

  makeTracer("sparql")
  makeTracer("sparql-indexing")

  addRemoteContextResolution(contexts.definition)

  make[SparqlClient].named("sparql-indexing-client").fromResource {
    (cfg: BlazegraphViewsConfig, metricsClient: OtelMetricsClient, tracer: Tracer[IO] @Id("sparql-indexing")) =>
      SparqlClient(cfg.access, metricsClient, "sparql-indexing")(using tracer)
  }

  make[SparqlClient].named("sparql-query-client").fromResource {
    (cfg: BlazegraphViewsConfig, metricsClient: OtelMetricsClient, tracer: Tracer[IO] @Id("sparql")) =>
      SparqlClient(cfg.access, metricsClient, "sparql-query")(using tracer)
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
          uuidF: UUIDF,
          tracer: Tracer[IO] @Id("sparql")
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
        )(using uuidF)(using tracer)
    }

  make[CurrentActiveViews].from { (views: BlazegraphViews) =>
    CurrentActiveViews(views)
  }

  make[SparqlHealthCheck].fromEffect {
    (
        client: SparqlClient @Id("sparql-indexing-client"),
        supervisor: Supervisor
    ) =>
      SparqlHealthCheck(client, supervisor)
  }

  make[SparqlProjectionLifeCycle].from {
    (
        graphStream: GraphResourceStream,
        pipeChainCompiler: PipeChainCompiler,
        healthCheck: SparqlHealthCheck,
        client: SparqlClient @Id("sparql-indexing-client"),
        config: BlazegraphViewsConfig,
        baseUri: BaseUri,
        tracer: Tracer[IO] @Id("sparql-indexing")
    ) =>
      SparqlProjectionLifeCycle(
        graphStream,
        pipeChainCompiler,
        healthCheck,
        client,
        config.retryStrategy,
        config.batch
      )(using baseUri, tracer)
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
        cfg: BlazegraphViewsConfig,
        xas: Transactors,
        tracer: Tracer[IO] @Id("sparql")
    ) =>
      BlazegraphViewsQuery(aclCheck, views, client, cfg.prefix, xas)(using tracer)
  }

  make[IncomingOutgoingLinks].fromEffect {
    (
        fetchContext: FetchContext,
        views: BlazegraphViews,
        client: SparqlClient @Id("sparql-query-client"),
        base: BaseUri,
        tracer: Tracer[IO] @Id("sparql")
    ) =>
      Queries.load.map { queries =>
        IncomingOutgoingLinks(fetchContext, views, client, queries)(using base, tracer)
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
        fusionConfig: FusionConfig,
        tracer: Tracer[IO] @Id("sparql")
    ) =>
      new BlazegraphViewsRoutes(
        views,
        viewsQuery,
        incomingOutgoingLinks,
        identities,
        aclCheck
      )(using baseUri, cr, ordering, cfg.pagination, fusionConfig, tracer)
  }

  make[BlazegraphViewsIndexingRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        views: BlazegraphViews,
        sparqlRestartScheduler: SparqlRestartScheduler,
        projectionDirectives: ProjectionsDirectives,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("sparql")
    ) =>
      new BlazegraphViewsIndexingRoutes(
        views.fetchIndexingView(_, _),
        sparqlRestartScheduler,
        identities,
        aclCheck,
        projectionDirectives
      )(using cr, ordering, tracer)
  }

  make[SparqlSupervisionRoutes].from {
    (
        sparqlSupervision: SparqlSupervision,
        identities: Identities,
        aclCheck: AclCheck,
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("sparql")
    ) =>
      new SparqlSupervisionRoutes(sparqlSupervision, identities, aclCheck)(using ordering, tracer)
  }

  make[BlazegraphScopeInitialization].from {
    (
        views: BlazegraphViews,
        serviceAccount: ServiceAccount,
        config: BlazegraphViewsConfig,
        tracer: Tracer[IO] @Id("sparql")
    ) =>
      new BlazegraphScopeInitialization(views, serviceAccount, config.defaults)(using tracer)
  }
  many[ScopeInitialization].ref[BlazegraphScopeInitialization]

  many[ProjectDeletionTask].add { (currentActiveViews: CurrentActiveViews, views: BlazegraphViews) =>
    BlazegraphDeletionTask(currentActiveViews, views)
  }

  many[ViewsList].add { (views: BlazegraphViews) => ViewsList(views.list) }

  many[SseEncoder[?]].add { (base: BaseUri) => BlazegraphViewEvent.sseEncoder(using base) }

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
        )(using baseUri),
        requiresStrictEntity = true
      )
  }

  many[ServiceDependency].add { (client: SparqlClient @Id("sparql-indexing-client")) =>
    new SparqlServiceDependency(client)
  }

  many[SyncIndexingAction].add {
    (
        shifts: ResourceShifts,
        currentActiveViews: CurrentActiveViews,
        pipeChainCompiler: PipeChainCompiler,
        client: SparqlClient @Id("sparql-indexing-client"),
        config: BlazegraphViewsConfig,
        baseUri: BaseUri,
        tracer: Tracer[IO] @Id("sparql-indexing")
    ) =>
      SparqlIndexingAction(shifts, currentActiveViews, pipeChainCompiler, client, config.syncIndexingTimeout)(using
        baseUri,
        tracer
      )
  }
}
