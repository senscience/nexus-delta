package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ExportRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sdk.resources.{Resources, ResourcesExporter}
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.{Scope, Transactors}
import ai.senscience.nexus.delta.sourcing.exporter.{ExportConfig, Exporter}
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

// $COVERAGE-OFF$
object ExportModule extends NexusModuleDef {

  makeConfig[ExportConfig]("app.export")

  makeTracer("export")

  make[Exporter].fromEffect { (config: ExportConfig, xas: Transactors) =>
    Exporter(config, xas)
  }

  make[ResourcesExporter].fromEffect {
    (
        config: ExportConfig,
        resources: Resources,
        projects: Projects,
        clock: Clock[IO],
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate")
    ) =>
      ResourcesExporter(resources, projects.currentRefs(Scope.root), clock, config.nquads)(using baseUri, cr)
  }

  make[ExportRoutes].from {
    (
        baseUri: BaseUri,
        identities: Identities,
        aclCheck: AclCheck,
        exporter: Exporter,
        resourcesExporter: ResourcesExporter,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("export")
    ) =>
      new ExportRoutes(identities, aclCheck, exporter, resourcesExporter)(using baseUri)(using cr, ordering, tracer)
  }

  many[PriorityRoute].add { (route: ExportRoutes) =>
    PriorityRoute(pluginsMaxPriority + 1, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
