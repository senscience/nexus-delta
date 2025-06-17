package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.config.AppConfig
import ai.senscience.nexus.delta.routes.TypeHierarchyRoutes
import ai.senscience.nexus.delta.sdk.PriorityRoute
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.typehierarchy.TypeHierarchy
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import ch.epfl.bluebrain.nexus.delta.kernel.utils.ClasspathResourceLoader
import ch.epfl.bluebrain.nexus.delta.rdf.Vocabulary.contexts
import ch.epfl.bluebrain.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ch.epfl.bluebrain.nexus.delta.rdf.utils.JsonKeyOrdering
import izumi.distage.model.definition.{Id, ModuleDef}

object TypeHierarchyModule extends ModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  make[TypeHierarchy].from { (xas: Transactors, config: AppConfig, clock: Clock[IO]) =>
    TypeHierarchy(xas, config.typeHierarchy, clock)
  }

  make[TypeHierarchyRoutes].from {
    (
        identities: Identities,
        typeHierarchy: TypeHierarchy,
        aclCheck: AclCheck,
        config: AppConfig,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new TypeHierarchyRoutes(
        typeHierarchy,
        identities,
        aclCheck
      )(config.http.baseUri, cr, ordering)
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
