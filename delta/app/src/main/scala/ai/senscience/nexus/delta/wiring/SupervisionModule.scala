package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.{EventMetricsRoutes, IndexingSupervisionRoutes, SupervisionRoutes}
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.{ProjectHealer, ProjectsHealth}
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.projections.ProjectionErrors
import ai.senscience.nexus.delta.sourcing.stream.{ProjectActivitySignals, Supervisor}
import cats.effect.IO
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Supervision module wiring config.
  */
// $COVERAGE-OFF$
object SupervisionModule extends NexusModuleDef {

  makeTracer("supervision")

  make[SupervisionRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        supervisor: Supervisor,
        baseUri: BaseUri,
        jo: JsonKeyOrdering,
        projectsHealth: ProjectsHealth,
        projectHealer: ProjectHealer,
        projectActivitySignals: ProjectActivitySignals,
        tracer: Tracer[IO] @Id("supervision")
    ) =>
      new SupervisionRoutes(
        identities,
        aclCheck,
        supervisor.getRunningProjections(),
        projectsHealth,
        projectHealer,
        projectActivitySignals
      )(using baseUri)(using jo, tracer)
  }

  many[PriorityRoute].add { (route: SupervisionRoutes) =>
    PriorityRoute(pluginsMaxPriority + 12, route.routes, requiresStrictEntity = true)
  }

  make[IndexingSupervisionRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        projectionErrors: ProjectionErrors,
        baseUri: BaseUri,
        jo: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("supervision")
    ) =>
      new IndexingSupervisionRoutes(
        identities,
        aclCheck,
        projectionErrors
      )(using baseUri)(using jo, tracer)
  }

  many[PriorityRoute].add { (route: IndexingSupervisionRoutes) =>
    PriorityRoute(pluginsMaxPriority + 12, route.routes, requiresStrictEntity = true)
  }

  make[EventMetricsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        projectionsDirectives: ProjectionsDirectives,
        baseUri: BaseUri,
        tracer: Tracer[IO] @Id("supervision")
    ) =>
      new EventMetricsRoutes(
        identities,
        aclCheck,
        projectionsDirectives
      )(using baseUri)(using tracer)
  }

  many[PriorityRoute].add { (route: EventMetricsRoutes) =>
    PriorityRoute(pluginsMaxPriority + 12, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
