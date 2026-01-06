package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.RealmsRoutes
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.realms.*
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.otel4s.trace.Tracer

/**
  * Realms module wiring config.
  */
// $COVERAGE-OFF$
object RealmsModule extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[RealmsConfig]("app.realms")

  makeTracer("realms")

  addRemoteContextResolution(contexts.definition)

  make[Realms].from {
    (
        cfg: RealmsConfig,
        clock: Clock[IO],
        client: Client[IO] @Id("realm"),
        xas: Transactors,
        tracer: Tracer[IO] @Id("realms")
    ) =>
      RealmsImpl(cfg, WellKnownResolver(client), xas, clock)(using tracer)
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
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("realms")
    ) =>
      new RealmsRoutes(identities, realms, aclCheck)(using baseUri)(using cfg.pagination, cr, ordering, tracer)
  }

  make[Client[IO]].named("realm").fromResource(EmberClientBuilder.default[IO].build)

  many[PriorityRoute].add { (route: RealmsRoutes) =>
    PriorityRoute(pluginsMaxPriority + 4, route.routes, requiresStrictEntity = true)
  }

}
// $COVERAGE-ON$
