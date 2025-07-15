package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ProjectsRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.FlattenedAclStore
import ai.senscience.nexus.delta.sdk.deletion.{ProjectDeletionCoordinator, ProjectDeletionTask}
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.organizations.FetchActiveOrganization
import ai.senscience.nexus.delta.sdk.projects.*
import ai.senscience.nexus.delta.sdk.projects.job.ProjectHealthJob
import ai.senscience.nexus.delta.sdk.projects.model.*
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.partition.DatabasePartitioner
import ai.senscience.nexus.delta.sourcing.projections.ProjectLastUpdateStore
import ai.senscience.nexus.delta.sourcing.stream.Supervisor
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id

/**
  * Projects wiring
  */
@SuppressWarnings(Array("UnsafeTraversableMethods"))
object ProjectsModule extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  final case class ApiMappingsCollection(value: Set[ApiMappings]) {
    def merge: ApiMappings = value.foldLeft(ApiMappings.empty)(_ + _)
  }

  makeConfig[ProjectsConfig]("app.projects")

  make[ApiMappingsCollection].from { (mappings: Set[ApiMappings]) =>
    ApiMappingsCollection(mappings)
  }

  make[Projects].fromEffect {
    (
        config: ProjectsConfig,
        databasePartitioner: DatabasePartitioner,
        scopeInitializer: ScopeInitializer,
        mappings: ApiMappingsCollection,
        xas: Transactors,
        clock: Clock[IO],
        uuidF: UUIDF
    ) =>
      IO.pure(
        ProjectsImpl(
          FetchActiveOrganization(xas),
          databasePartitioner.onCreateProject,
          ValidateProjectDeletion(xas, config.deletion.enabled),
          scopeInitializer,
          mappings.merge,
          config.eventLog,
          xas,
          clock
        )(uuidF)
      )
  }

  make[ProjectScopeResolver].from { (projects: Projects, flattenedAclStore: FlattenedAclStore) =>
    ProjectScopeResolver(projects, flattenedAclStore)
  }

  make[ProjectsHealth].from { (errorStore: ScopeInitializationErrorStore) =>
    ProjectsHealth(errorStore)
  }

  make[ProjectHealer].from(
    (errorStore: ScopeInitializationErrorStore, scopeInitializer: ScopeInitializer, serviceAccount: ServiceAccount) =>
      ProjectHealer(errorStore, scopeInitializer, serviceAccount)
  )

  make[ProjectHealthJob].fromEffect { (projects: Projects, projectHealer: ProjectHealer) =>
    ProjectHealthJob(projects, projectHealer)
  }

  make[ProjectsStatistics].fromEffect { (xas: Transactors) =>
    ProjectsStatistics(xas)
  }

  make[FetchContext].from { (mappings: ApiMappingsCollection, xas: Transactors) =>
    FetchContext(mappings.merge, xas)
  }

  make[ProjectDeletionCoordinator].fromEffect {
    (
        projects: Projects,
        databasePartitioner: DatabasePartitioner,
        deletionTasks: Set[ProjectDeletionTask],
        config: ProjectsConfig,
        serviceAccount: ServiceAccount,
        supervisor: Supervisor,
        projectLastUpdateStore: ProjectLastUpdateStore,
        xas: Transactors,
        clock: Clock[IO]
    ) =>
      ProjectDeletionCoordinator(
        projects,
        databasePartitioner,
        deletionTasks,
        config.deletion,
        serviceAccount,
        supervisor,
        projectLastUpdateStore,
        xas,
        clock
      )
  }

  make[DeltaSchemeDirectives].from { (fetchContext: FetchContext) => DeltaSchemeDirectives(fetchContext) }

  make[ProjectsRoutes].from {
    (
        config: ProjectsConfig,
        identities: Identities,
        aclCheck: AclCheck,
        projects: Projects,
        projectScopeResolver: ProjectScopeResolver,
        projectsStatistics: ProjectsStatistics,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig
    ) =>
      new ProjectsRoutes(identities, aclCheck, projects, projectScopeResolver, projectsStatistics)(
        baseUri,
        config.pagination,
        config.prefix,
        cr,
        ordering,
        fusionConfig
      )
  }

  many[SseEncoder[?]].add { base: BaseUri => ProjectEvent.sseEncoder(base) }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/projects-metadata.json"))

  many[RemoteContextResolution].addEffect(
    for {
      projectsCtx     <- ContextValue.fromFile("contexts/projects.json")
      projectsMetaCtx <- ContextValue.fromFile("contexts/projects-metadata.json")
    } yield RemoteContextResolution.fixed(
      contexts.projects         -> projectsCtx,
      contexts.projectsMetadata -> projectsMetaCtx
    )
  )

  many[PriorityRoute].add { (route: ProjectsRoutes) =>
    PriorityRoute(pluginsMaxPriority + 7, route.routes, requiresStrictEntity = true)
  }

}
