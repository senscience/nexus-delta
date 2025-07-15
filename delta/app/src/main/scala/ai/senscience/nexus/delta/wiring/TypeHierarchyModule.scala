package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.config.Configs
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.TypeHierarchyRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.typehierarchy.{TypeHierarchy, TypeHierarchyConfig}
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.{Id, ModuleDef}

object TypeHierarchyModule extends ModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  make[TypeHierarchyConfig].from(Configs.load[TypeHierarchyConfig](_, "app.type-hierarchy"))

  make[TypeHierarchy].from { (xas: Transactors, config: TypeHierarchyConfig, clock: Clock[IO]) =>
    TypeHierarchy(xas, config, clock)
  }

  make[TypeHierarchyRoutes].from {
    (
        identities: Identities,
        typeHierarchy: TypeHierarchy,
        aclCheck: AclCheck,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new TypeHierarchyRoutes(
        typeHierarchy,
        identities,
        aclCheck
      )(baseUri, cr, ordering)
  }

  many[RemoteContextResolution].addEffect(
    for {
      typeHierarchyCtx <- ContextValue.fromFile("contexts/type-hierarchy.json")
    } yield RemoteContextResolution.fixed(
      contexts.typeHierarchy -> typeHierarchyCtx
    )
  )

  many[PriorityRoute].add { (route: TypeHierarchyRoutes) =>
    PriorityRoute(pluginsMaxPriority + 14, route.routes, requiresStrictEntity = true)
  }

}
