package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.plugins.graph.analytics.config.GraphAnalyticsConfig
import ai.senscience.nexus.delta.plugins.graph.analytics.indexing.GraphAnalyticsStream
import ai.senscience.nexus.delta.plugins.graph.analytics.routes.GraphAnalyticsRoutes
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.projects.{FetchContext, Projects}
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.projections.Projections
import ai.senscience.nexus.delta.sourcing.query.{ElemStreaming, SelectFilter}
import ai.senscience.nexus.delta.sourcing.stream.Supervisor
import izumi.distage.model.definition.{Id, ModuleDef}

/**
  * Graph analytics plugin wiring.
  */
class GraphAnalyticsPluginModule(priority: Int) extends ModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  make[GraphAnalyticsConfig].from { GraphAnalyticsConfig.load _ }

  make[GraphAnalytics]
    .from { (client: ElasticSearchClient, fetchContext: FetchContext, config: GraphAnalyticsConfig) =>
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
        client: ElasticSearchClient,
        config: GraphAnalyticsConfig
    ) =>
      GraphAnalyticsCoordinator(projects, analyticsStream, supervisor, client, config)
  }

  make[GraphAnalyticsViewsQuery].from { (client: ElasticSearchClient, config: GraphAnalyticsConfig) =>
    new GraphAnalyticsViewsQueryImpl(config.prefix, client)
  }

  make[GraphAnalyticsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        graphAnalytics: GraphAnalytics,
        projections: Projections,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        viewsQuery: GraphAnalyticsViewsQuery
    ) =>
      new GraphAnalyticsRoutes(
        identities,
        aclCheck,
        graphAnalytics,
        project => projections.statistics(project, SelectFilter.latest, GraphAnalytics.projectionName(project)),
        viewsQuery
      )(
        baseUri,
        cr,
        ordering
      )
  }

  many[RemoteContextResolution].addEffect {
    for {
      relationshipsCtx <- ContextValue.fromFile("contexts/relationships.json")
      propertiesCtx    <- ContextValue.fromFile("contexts/properties.json")
    } yield RemoteContextResolution.fixed(
      contexts.relationships -> relationshipsCtx,
      contexts.properties    -> propertiesCtx
    )
  }

  many[PriorityRoute].add { (route: GraphAnalyticsRoutes) =>
    PriorityRoute(priority, route.routes, requiresStrictEntity = true)
  }
}
