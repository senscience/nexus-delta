package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.{ElemRoutes, EventsRoutes}
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.organizations.Organizations
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sdk.sse.{SseConfig, SseElemStream, SseEncoder, SseEventLog}
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming
import izumi.distage.model.definition.Id

/**
  * Events wiring
  */
object EventsModule extends NexusModuleDef {

  makeConfig[SseConfig]("app.sse")

  make[SseEventLog].fromEffect {
    (
        config: SseConfig,
        organizations: Organizations,
        projects: Projects,
        sseEncoders: Set[SseEncoder[?]],
        xas: Transactors,
        jo: JsonKeyOrdering
    ) =>
      SseEventLog(
        sseEncoders,
        organizations.fetch(_).void,
        projects.fetch(_).void,
        config,
        xas
      )(jo)
  }

  make[SseElemStream].from { (elemStreaming: ElemStreaming) => SseElemStream(elemStreaming) }

  make[EventsRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        sseEventLog: SseEventLog,
        baseUri: BaseUri
    ) =>
      new EventsRoutes(identities, aclCheck, sseEventLog)(baseUri)
  }

  many[PriorityRoute].add { (route: EventsRoutes) =>
    PriorityRoute(pluginsMaxPriority + 11, route.routes, requiresStrictEntity = true)
  }

  make[ElemRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        sseElemStream: SseElemStream,
        schemeDirectives: DeltaSchemeDirectives,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new ElemRoutes(identities, aclCheck, sseElemStream, schemeDirectives)(baseUri, cr, ordering)
  }

  many[PriorityRoute].add { (route: ElemRoutes) =>
    PriorityRoute(pluginsMaxPriority + 12, route.routes, requiresStrictEntity = true)
  }
}
