package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.routes.PermissionsRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.RouteContext
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.permissions.{contexts, Permissions, PermissionsConfig, PermissionsImpl}
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Permissions module wiring config.
  */
// $COVERAGE-OFF$
object PermissionsModule extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[PermissionsConfig]("app.permissions")

  makeTracer("permissions")

  addRemoteContextResolution(contexts.definition)

  make[Permissions].from {
    (cfg: PermissionsConfig, xas: Transactors, clock: Clock[IO], tracer: Tracer[IO] @Id("permissions")) =>
      PermissionsImpl(cfg, xas, clock)(using tracer)
  }

  make[PermissionsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        permissions: Permissions,
        ctx: RouteContext,
        tracer: Tracer[IO] @Id("permissions")
    ) => new PermissionsRoutes(identities, aclCheck, permissions)(using ctx, tracer)
  }

  many[RouteEntry].add { (route: PermissionsRoutes) =>
    RouteEntry(pluginsMaxPriority + 3, route.routes, requiresStrictEntity = true)
  }
}
// $COVERAGE-ON$
