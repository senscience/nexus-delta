package ai.senscience.nexus.delta.projectdeletion

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.projectdeletion.model.{contexts, ProjectDeletionConfig}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.{Projects, ProjectsStatistics}
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.stream.Supervisor
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id

class ProjectDeletionModule(priority: Int) extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[ProjectDeletionConfig]("plugins.project-deletion")

  many[RemoteContextResolution].addEffect {
    ContextValue.fromFile("contexts/project-deletion.json").map { ctx =>
      RemoteContextResolution.fixed(contexts.projectDeletion -> ctx)
    }
  }

  make[ProjectDeletionRoutes].from {
    (
        config: ProjectDeletionConfig,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) => new ProjectDeletionRoutes(config)(baseUri, cr, ordering)
  }

  many[PriorityRoute].add { (route: ProjectDeletionRoutes) =>
    PriorityRoute(priority, route.routes, requiresStrictEntity = true)
  }

  make[ProjectDeletionRunner].fromEffect {
    (
        projects: Projects,
        config: ProjectDeletionConfig,
        projectStatistics: ProjectsStatistics,
        supervisor: Supervisor,
        clock: Clock[IO]
    ) => ProjectDeletionRunner.start(projects, config, projectStatistics, supervisor, clock)
  }
}
