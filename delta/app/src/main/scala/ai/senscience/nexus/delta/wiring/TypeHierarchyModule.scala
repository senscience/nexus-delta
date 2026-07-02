package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.routes.TypeHierarchyRoutes
import ai.senscience.nexus.delta.sdk.RouteEntry
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.RouteContext
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.typehierarchy.{contexts, TypeHierarchy, TypeHierarchyConfig}
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

object TypeHierarchyModule extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[TypeHierarchyConfig]("app.type-hierarchy")

  makeTracer("type-hierarchy")

  addRemoteContextResolution(contexts.definition)

  make[TypeHierarchy].from { (xas: Transactors, config: TypeHierarchyConfig, clock: Clock[IO]) =>
    TypeHierarchy(xas, config, clock)
  }

  make[TypeHierarchyRoutes].from {
    (
        identities: Identities,
        typeHierarchy: TypeHierarchy,
        aclCheck: AclCheck,
        ctx: RouteContext,
        tracer: Tracer[IO] @Id("type-hierarchy")
    ) =>
      new TypeHierarchyRoutes(
        typeHierarchy,
        identities,
        aclCheck
      )(using ctx, tracer)
  }

  many[RouteEntry].add { (route: TypeHierarchyRoutes) =>
    RouteEntry(pluginsMaxPriority + 14, route.routes, requiresStrictEntity = true)
  }

}
