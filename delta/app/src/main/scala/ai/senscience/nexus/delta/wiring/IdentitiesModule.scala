package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.cache.CacheConfig
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.IdentitiesRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.auth.{AuthTokenProvider, OpenIdAuthService}
import ai.senscience.nexus.delta.sdk.identities.{Identities, IdentitiesImpl}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.http4s.client.Client

/**
  * Identities module wiring config.
  */
// $COVERAGE-OFF$
object IdentitiesModule extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[CacheConfig]("app.identities")

  make[Identities].fromEffect { (realms: Realms, client: Client[IO] @Id("realm"), config: CacheConfig) =>
    IdentitiesImpl(realms, client, config)
  }

  make[AuthTokenProvider].fromEffect { (client: Client[IO] @Id("realm"), realms: Realms, clock: Clock[IO]) =>
    val authService = new OpenIdAuthService(client, realms)
    AuthTokenProvider(authService, clock)
  }

  many[RemoteContextResolution].addEffect(ContextValue.fromFile("contexts/identities.json").map { ctx =>
    RemoteContextResolution.fixed(contexts.identities -> ctx)
  })

  make[IdentitiesRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) => new IdentitiesRoutes(identities, aclCheck)(baseUri, cr, ordering)

  }

  many[PriorityRoute].add { (route: IdentitiesRoutes) =>
    PriorityRoute(pluginsMaxPriority + 2, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
