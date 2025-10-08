package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViews
import ai.senscience.nexus.delta.plugins.compositeviews.config.CompositeViewsConfig
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeProjectionLifeCycle
import ai.senscience.nexus.delta.plugins.search.model.{defaulMappings, SearchConfig}
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import cats.effect.IO
import com.typesafe.config.Config
import io.circe.syntax.EncoderOps
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

class SearchPluginModule(priority: Int) extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  make[SearchConfig].fromEffect { (cfg: Config) => SearchConfig.load(cfg) }

  makeTracer("search")

  make[Search].from {
    (
        compositeViews: CompositeViews,
        aclCheck: AclCheck,
        esClient: ElasticSearchClient,
        compositeConfig: CompositeViewsConfig,
        searchConfig: SearchConfig
    ) =>
      Search(compositeViews, aclCheck, esClient, compositeConfig.prefix, searchConfig.suites)
  }

  make[SearchScopeInitialization].from {
    (
        views: CompositeViews,
        config: SearchConfig,
        serviceAccount: ServiceAccount,
        baseUri: BaseUri,
        tracer: Tracer[IO] @Id("search")
    ) =>
      new SearchScopeInitialization(views, config.indexing, serviceAccount, config.defaults)(using baseUri, tracer)
  }
  many[ScopeInitialization].ref[SearchScopeInitialization]

  many[RemoteContextResolution].addEffect(
    ContextValue.fromFile("contexts/suites.json").map { suitesCtx =>
      RemoteContextResolution.fixed(contexts.suites -> suitesCtx)
    }
  )

  many[ApiMappings].add(defaulMappings)

  make[SearchRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        search: Search,
        config: SearchConfig,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) => new SearchRoutes(identities, aclCheck, search, config.fields.asJson, config.suites)(baseUri, cr, ordering)
  }

  many[PriorityRoute].add { (route: SearchRoutes) =>
    PriorityRoute(priority, route.routes, requiresStrictEntity = true)
  }

  many[CompositeProjectionLifeCycle.Hook].add {
    (
        compositeViews: CompositeViews,
        config: SearchConfig,
        baseUri: BaseUri,
        serviceAccount: ServiceAccount
    ) => SearchConfigHook(compositeViews, config.defaults, config.indexing)(baseUri, serviceAccount.subject)
  }

}
