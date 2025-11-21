package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.plugins.graph.analytics.config.GraphAnalyticsConfig
import ai.senscience.nexus.delta.plugins.graph.analytics.indexing.GraphAnalyticsStream
import ai.senscience.nexus.delta.plugins.graph.analytics.routes.GraphAnalyticsRoutes
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.projects.{FetchContext, Projects}
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming
import ai.senscience.nexus.delta.sourcing.stream.{ProjectionBackpressure, Supervisor}
import cats.effect.IO
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Graph analytics plugin wiring.
  */
class GraphAnalyticsPluginModule(priority: Int) extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[GraphAnalyticsConfig]("plugins.graph-analytics")

  makeTracer("graph-analytics")

  addRemoteContextResolution(contexts.definition)

  make[ProjectionBackpressure].named("ga-backpressure").fromEffect { (cfg: GraphAnalyticsConfig) =>
    ProjectionBackpressure(cfg.backpressure)
  }

  make[GraphAnalytics]
    .from {
      (
          client: ElasticSearchClient @Id("elasticsearch-query-client"),
          fetchContext: FetchContext,
          config: GraphAnalyticsConfig
      ) =>
        GraphAnalytics(client, fetchContext, config.prefix, config.termAggregations)
    }

  make[GraphAnalyticsStream].from { (elemStreaming: ElemStreaming, xas: Transactors) =>
    GraphAnalyticsStream(elemStreaming, xas)
  }

  make[GraphAnalyticsCoordinator].fromEffect {
    (
        projects: Projects,
        analyticsStream: GraphAnalyticsStream,
        supervisor: Supervisor,
        client: ElasticSearchClient @Id("elasticsearch-indexing-client"),
        config: GraphAnalyticsConfig,
        backpressure: ProjectionBackpressure @Id("ga-backpressure"),
        tracer: Tracer[IO] @Id("graph-analytics")
    ) =>
      GraphAnalyticsCoordinator(projects, analyticsStream, supervisor, client, config)(using backpressure, tracer)
  }

  make[GraphAnalyticsViewsQuery].from {
    (client: ElasticSearchClient @Id("elasticsearch-query-client"), config: GraphAnalyticsConfig) =>
      new GraphAnalyticsViewsQueryImpl(config.prefix, client)
  }

  make[GraphAnalyticsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        graphAnalytics: GraphAnalytics,
        projectionsDirectives: ProjectionsDirectives,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        viewsQuery: GraphAnalyticsViewsQuery,
        tracer: Tracer[IO] @Id("graph-analytics")
    ) =>
      new GraphAnalyticsRoutes(
        identities,
        aclCheck,
        graphAnalytics,
        projectionsDirectives,
        viewsQuery
      )(using baseUri)(using cr, ordering, tracer)
  }

  many[PriorityRoute].add { (route: GraphAnalyticsRoutes) =>
    PriorityRoute(priority, route.routes, requiresStrictEntity = true)
  }
}
