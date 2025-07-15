package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.plugins.archive.model.contexts
import ai.senscience.nexus.delta.plugins.archive.routes.ArchiveRoutes
import ai.senscience.nexus.delta.plugins.storage.FileSelf
import ai.senscience.nexus.delta.plugins.storage.files.Files
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
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

/**
  * Archive plugin wiring.
  */
object ArchivePluginModule extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[ArchivePluginConfig]("plugins.archive")

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
        clock: Clock[IO]
    ) =>
      Archives(fetchContext, archiveDownload, cfg, xas, clock)(uuidF, rcr)
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

  many[RemoteContextResolution].addEffect {
    for {
      ctx     <- ContextValue.fromFile("contexts/archives.json")
      metaCtx <- ContextValue.fromFile("contexts/archives-metadata.json")
    } yield RemoteContextResolution.fixed(
      contexts.archives         -> ctx,
      contexts.archivesMetadata -> metaCtx
    )

  }

  many[ApiMappings].add(Archives.mappings)

}
