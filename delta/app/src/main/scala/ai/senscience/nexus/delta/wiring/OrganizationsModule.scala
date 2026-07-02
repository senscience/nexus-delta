package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.routes.OrganizationsRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.{AclCheck, Acls}
import ai.senscience.nexus.delta.sdk.directives.RouteContext
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.organizations.*
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.partition.DatabasePartitioner
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Organizations module wiring config.
  */
// $COVERAGE-OFF$
object OrganizationsModule extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[OrganizationsConfig]("app.organizations")

  makeTracer("orgs")

  addRemoteContextResolution(contexts.definition)

  make[Organizations].from {
    (
        config: OrganizationsConfig,
        scopeInitializer: ScopeInitializer,
        clock: Clock[IO],
        uuidF: UUIDF,
        xas: Transactors,
        tracer: Tracer[IO] @Id("orgs")
    ) =>
      OrganizationsImpl(
        scopeInitializer,
        config.eventLog,
        xas,
        clock
      )(using uuidF, tracer)
  }

  make[OrganizationDeleter].from {
    (acls: Acls, orgs: Organizations, projects: Projects, databasePartitioner: DatabasePartitioner) =>
      OrganizationDeleter(acls, orgs, projects, databasePartitioner)
  }

  make[OrganizationsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        organizations: Organizations,
        orgDeleter: OrganizationDeleter,
        orgConfig: OrganizationsConfig,
        ctx: RouteContext,
        tracer: Tracer[IO] @Id("orgs")
    ) =>
      new OrganizationsRoutes(identities, aclCheck, organizations, orgDeleter)(using
        ctx,
        orgConfig.pagination,
        tracer
      )
  }

  many[RouteEntry].add { (route: OrganizationsRoutes) =>
    RouteEntry(
      pluginsMaxPriority + 6,
      route.routes,
      requiresStrictEntity = true,
      classifier = OrganizationsRoutes.classifier
    )
  }

}
// $COVERAGE-ON$
