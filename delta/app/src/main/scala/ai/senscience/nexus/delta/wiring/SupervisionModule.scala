package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.SupervisionRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.{ProjectHealer, ProjectsHealth}
import ai.senscience.nexus.delta.sourcing.stream.{ProjectActivitySignals, Supervisor}
import izumi.distage.model.definition.{Id, ModuleDef}

/**
  * Supervision module wiring config.
  */
// $COVERAGE-OFF$
object SupervisionModule extends ModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  make[SupervisionRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        supervisor: Supervisor,
        baseUri: BaseUri,
        rc: RemoteContextResolution @Id("aggregate"),
        jo: JsonKeyOrdering,
        projectsHealth: ProjectsHealth,
        projectHealer: ProjectHealer,
        projectActivitySignals: ProjectActivitySignals
    ) =>
      new SupervisionRoutes(
        identities,
        aclCheck,
        supervisor.getRunningProjections(),
        projectsHealth,
        projectHealer,
        projectActivitySignals
      )(baseUri, rc, jo)
  }

  many[RemoteContextResolution].addEffect(
    for {
      supervisionCtx <- ContextValue.fromFile("contexts/supervision.json")
    } yield RemoteContextResolution.fixed(
      contexts.supervision -> supervisionCtx
    )
  )

  many[PriorityRoute].add { (route: SupervisionRoutes) =>
    PriorityRoute(pluginsMaxPriority + 12, route.routes, requiresStrictEntity = true)
  }
}
// $COVERAGE-ON$
