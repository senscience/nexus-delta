package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.RealmsRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.model.{BaseUri, MetadataContextValue}
import ai.senscience.nexus.delta.sdk.realms.*
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

/**
  * Realms module wiring config.
  */
// $COVERAGE-OFF$
object RealmsModule extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[RealmsConfig]("app.realms")

  make[Realms].from {
    (
        cfg: RealmsConfig,
        clock: Clock[IO],
        client: Client[IO] @Id("realm"),
        xas: Transactors
    ) =>
      val wellKnownResolver = WellKnownResolver(client) _
      RealmsImpl(cfg, wellKnownResolver, xas, clock)
  }

  make[RealmProvisioning].from { (realms: Realms, cfg: RealmsConfig, serviceAccount: ServiceAccount) =>
    new RealmProvisioning(realms, cfg.provisioning, serviceAccount)
  }

  make[RealmsRoutes].from {
    (
        identities: Identities,
        realms: Realms,
        cfg: RealmsConfig,
        aclCheck: AclCheck,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new RealmsRoutes(identities, realms, aclCheck)(baseUri, cfg.pagination, cr, ordering)
  }

  make[Client[IO]].named("realm").fromResource(EmberClientBuilder.default[IO].build)

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/realms-metadata.json"))

  many[RemoteContextResolution].addEffect(
    for {
      realmCtx      <- ContextValue.fromFile("contexts/realms.json")
      realmsMetaCtx <- ContextValue.fromFile("contexts/realms-metadata.json")
    } yield RemoteContextResolution.fixed(contexts.realms -> realmCtx, contexts.realmsMetadata -> realmsMetaCtx)
  )

  many[PriorityRoute].add { (route: RealmsRoutes) =>
    PriorityRoute(pluginsMaxPriority + 4, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
