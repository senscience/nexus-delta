package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.plugins.archive.contexts
import ai.senscience.nexus.delta.plugins.archive.routes.ArchiveRoutes
import ai.senscience.nexus.delta.plugins.storage.FileSelf
import ai.senscience.nexus.delta.plugins.storage.files.Files
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.{BaseUri, MetadataContextValue}
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Archive plugin wiring.
  */
object ArchivePluginModule extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[ArchivePluginConfig]("plugins.archive")

  makeTracer("archives")

  addRemoteContextResolution(contexts.definition)

  make[ArchiveDownload].from {
    (
        aclCheck: AclCheck,
        shifts: ResourceShifts,
        files: Files,
        fileSelf: FileSelf,
        sort: JsonKeyOrdering,
        baseUri: BaseUri,
        rcr: RemoteContextResolution @Id("aggregate")
    ) =>
      ArchiveDownload(aclCheck, shifts, files, fileSelf)(sort, baseUri, rcr)
  }

  make[FileSelf].from { (fetchContext: FetchContext, baseUri: BaseUri) =>
    FileSelf(fetchContext)(baseUri)
  }

  make[Archives].from {
    (
        fetchContext: FetchContext,
        archiveDownload: ArchiveDownload,
        cfg: ArchivePluginConfig,
        xas: Transactors,
        uuidF: UUIDF,
        rcr: RemoteContextResolution @Id("aggregate"),
        clock: Clock[IO],
        tracer: Tracer[IO] @Id("archives")
    ) =>
      Archives(fetchContext, archiveDownload, cfg, xas, clock)(using uuidF, rcr, tracer)
  }

  make[ArchiveRoutes].from {
    (
        archives: Archives,
        identities: Identities,
        aclCheck: AclCheck,
        baseUri: BaseUri,
        rcr: RemoteContextResolution @Id("aggregate"),
        jko: JsonKeyOrdering
    ) =>
      new ArchiveRoutes(archives, identities, aclCheck)(baseUri, rcr, jko)
  }

  many[PriorityRoute].add { (cfg: ArchivePluginConfig, routes: ArchiveRoutes) =>
    PriorityRoute(cfg.priority, routes.routes, requiresStrictEntity = true)
  }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/archives-metadata.json"))

  many[ApiMappings].add(Archives.mappings)

}
