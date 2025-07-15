package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.PermissionsRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.{BaseUri, MetadataContextValue}
import ai.senscience.nexus.delta.sdk.permissions.{Permissions, PermissionsConfig, PermissionsImpl}
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id

/**
  * Permissions module wiring config.
  */
// $COVERAGE-OFF$
object PermissionsModule extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[PermissionsConfig]("app.permissions")

  make[Permissions].from { (cfg: PermissionsConfig, xas: Transactors, clock: Clock[IO]) =>
    PermissionsImpl(cfg, xas, clock)
  }

  make[PermissionsRoutes].from {
    (
        identities: Identities,
        permissions: Permissions,
        aclCheck: AclCheck,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) => new PermissionsRoutes(identities, permissions, aclCheck)(baseUri, cr, ordering)
  }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/permissions-metadata.json"))

  many[RemoteContextResolution].addEffect(
    for {
      permissionsCtx     <- ContextValue.fromFile("contexts/permissions.json")
      permissionsMetaCtx <- ContextValue.fromFile("contexts/permissions-metadata.json")
    } yield RemoteContextResolution.fixed(
      contexts.permissions         -> permissionsCtx,
      contexts.permissionsMetadata -> permissionsMetaCtx
    )
  )

  many[PriorityRoute].add { (route: PermissionsRoutes) =>
    PriorityRoute(pluginsMaxPriority + 3, route.routes, requiresStrictEntity = true)
  }
}
// $COVERAGE-ON$
