package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.routes.IdentitiesRoutes
import ai.senscience.nexus.delta.sdk.RouteEntry
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.auth.{AuthTokenProvider, OpenIdAuthService}
import ai.senscience.nexus.delta.sdk.directives.RouteContext
import ai.senscience.nexus.delta.sdk.identities.{contexts, Identities, IdentitiesConfig, IdentitiesImpl}
import ai.senscience.nexus.delta.sdk.realms.Realms
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.http4s.client.Client
import org.typelevel.otel4s.trace.Tracer

/**
  * Identities module wiring config.
  */
// $COVERAGE-OFF$
object IdentitiesModule extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[IdentitiesConfig]("app.identities")

  makeTracer("identities")

  addRemoteContextResolution(contexts.definition)

  make[Identities].fromEffect {
    (realms: Realms, client: Client[IO] @Id("realm"), config: IdentitiesConfig, tracer: Tracer[IO] @Id("identities")) =>
      IdentitiesImpl(realms, client, config)(using tracer)
  }

  make[AuthTokenProvider].fromEffect { (client: Client[IO] @Id("realm"), realms: Realms, clock: Clock[IO]) =>
    val authService = new OpenIdAuthService(client, realms)
    AuthTokenProvider(authService, clock)
  }

  make[IdentitiesRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        ctx: RouteContext,
        tracer: Tracer[IO] @Id("identities")
    ) => new IdentitiesRoutes(identities, aclCheck)(using ctx, tracer)

  }

  many[RouteEntry].add { (route: IdentitiesRoutes) =>
    RouteEntry(pluginsMaxPriority + 2, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
