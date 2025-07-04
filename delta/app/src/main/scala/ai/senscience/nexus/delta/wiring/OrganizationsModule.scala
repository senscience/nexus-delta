package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.config.AppConfig
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.OrganizationsRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.{AclCheck, Acls}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.MetadataContextValue
import ai.senscience.nexus.delta.sdk.organizations.{OrganizationDeleter, Organizations, OrganizationsImpl}
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.partition.DatabasePartitioner
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.{Id, ModuleDef}

/**
  * Organizations module wiring config.
  */
// $COVERAGE-OFF$
object OrganizationsModule extends ModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  make[Organizations].from {
    (
        config: AppConfig,
        scopeInitializer: ScopeInitializer,
        clock: Clock[IO],
        uuidF: UUIDF,
        xas: Transactors
    ) =>
      OrganizationsImpl(
        scopeInitializer,
        config.organizations.eventLog,
        xas,
        clock
      )(uuidF)
  }

  make[OrganizationDeleter].from {
    (acls: Acls, orgs: Organizations, projects: Projects, databasePartitioner: DatabasePartitioner) =>
      OrganizationDeleter(acls, orgs, projects, databasePartitioner)
  }

  make[OrganizationsRoutes].from {
    (
        identities: Identities,
        organizations: Organizations,
        orgDeleter: OrganizationDeleter,
        cfg: AppConfig,
        aclCheck: AclCheck,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new OrganizationsRoutes(identities, organizations, orgDeleter, aclCheck)(
        cfg.http.baseUri,
        cfg.organizations.pagination,
        cr,
        ordering
      )
  }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/organizations-metadata.json"))

  many[RemoteContextResolution].addEffect(
    for {
      orgsCtx     <- ContextValue.fromFile("contexts/organizations.json")
      orgsMetaCtx <- ContextValue.fromFile("contexts/organizations-metadata.json")
    } yield RemoteContextResolution.fixed(
      contexts.organizations         -> orgsCtx,
      contexts.organizationsMetadata -> orgsMetaCtx
    )
  )

  many[PriorityRoute].add { (route: OrganizationsRoutes) =>
    PriorityRoute(pluginsMaxPriority + 6, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
