package ai.senscience.nexus.delta.plugins.storage

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.elasticsearch.metrics.MetricsIndexDef
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.plugins.storage.files.Files.FilesLog
import ai.senscience.nexus.delta.plugins.storage.files.contexts.files as fileCtxId
import ai.senscience.nexus.delta.plugins.storage.files.model.{File, FileEvent}
import ai.senscience.nexus.delta.plugins.storage.files.routes.{DelegateFilesRoutes, FilesRoutes, LinkFilesRoutes}
import ai.senscience.nexus.delta.plugins.storage.files.schemas.files as filesSchemaId
import ai.senscience.nexus.delta.plugins.storage.files.{Files, FormDataExtractor, MediaTypeDetector}
import ai.senscience.nexus.delta.plugins.storage.storages.*
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.{ShowFileLocation, StorageTypeConfig}
import ai.senscience.nexus.delta.plugins.storage.storages.access.{S3StorageAccess, StorageAccess}
import ai.senscience.nexus.delta.plugins.storage.storages.contexts.{storages as storageCtxId, storagesMetadata as storageMetaCtxId}
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageEvent
import ai.senscience.nexus.delta.plugins.storage.storages.operations.disk.DiskFileOperations
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.client.S3StorageClient
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.{S3FileOperations, S3LocationGenerator}
import ai.senscience.nexus.delta.plugins.storage.storages.operations.{FileOperations, LinkFileAction}
import ai.senscience.nexus.delta.plugins.storage.storages.routes.StoragesRoutes
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.IndexingAction.AggregateIndexingAction
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.jws.JWSPayloadHelper
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.metrics.ScopedEventMetricEncoder
import ai.senscience.nexus.delta.sdk.permissions.{Permissions, StoragePermissionProvider}
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.{ScopedEventLog, Transactors}
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives.concat
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.http4s.Uri.Path

/**
  * Storages and Files wiring
  */
class StoragePluginModule(priority: Int) extends NexusModuleDef {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[StoragePluginConfig]("plugins.storage")

  make[StorageTypeConfig].from { cfg: StoragePluginConfig => cfg.storages.storageTypeConfig }

  make[ShowFileLocation].from { cfg: StorageTypeConfig => cfg.showFileLocation }

  make[S3StorageClient].fromResource { (cfg: StoragePluginConfig) =>
    S3StorageClient.resource(cfg.storages.storageTypeConfig.amazon)
  }

  make[S3LocationGenerator].from { (cfg: StoragePluginConfig) =>
    val prefix: Path = cfg.storages.storageTypeConfig.amazon.flatMap(_.prefix).getOrElse(Path.empty)
    new S3LocationGenerator(prefix)
  }

  make[StorageAccess].from { (s3Client: S3StorageClient) => StorageAccess(S3StorageAccess(s3Client)) }

  make[Storages]
    .fromEffect {
      (
          fetchContext: FetchContext,
          contextResolution: ResolverContextResolution,
          storageAccess: StorageAccess,
          permissions: Permissions,
          xas: Transactors,
          cfg: StoragePluginConfig,
          serviceAccount: ServiceAccount,
          clock: Clock[IO],
          uuidF: UUIDF
      ) =>
        Storages(
          fetchContext,
          contextResolution,
          permissions.fetchPermissionSet,
          storageAccess,
          xas,
          cfg.storages,
          serviceAccount,
          clock
        )(uuidF)
    }

  make[FetchStorage].from { (storages: Storages, aclCheck: AclCheck) =>
    FetchStorage(storages, aclCheck)
  }

  make[StoragePermissionProvider].from { (storages: Storages) =>
    new StoragePermissionProviderImpl(storages)
  }

  make[StoragesStatistics].from {
    (
        client: ElasticSearchClient,
        storages: Storages,
        metricsIndex: MetricsIndexDef
    ) =>
      StoragesStatistics(
        client,
        storages.fetch(_, _).map(_.id),
        metricsIndex.name
      )
  }

  make[StoragesRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        storages: Storages,
        storagesStatistics: StoragesStatistics,
        schemeDirectives: DeltaSchemeDirectives,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig
    ) =>
      {
        new StoragesRoutes(
          identities,
          aclCheck,
          storages,
          storagesStatistics,
          schemeDirectives
        )(
          baseUri,
          cr,
          ordering,
          fusionConfig
        )
      }
  }

  make[FilesLog].from { (cfg: StoragePluginConfig, xas: Transactors, clock: Clock[IO]) =>
    ScopedEventLog(Files.definition(clock), cfg.files.eventLog, xas)
  }

  make[DiskFileOperations].from { (uuidF: UUIDF) => DiskFileOperations.mk(uuidF) }

  make[S3FileOperations].from { (client: S3StorageClient, locationGenerator: S3LocationGenerator, uuidF: UUIDF) =>
    S3FileOperations.mk(client, locationGenerator)(uuidF)
  }

  make[FileOperations].from { (disk: DiskFileOperations, s3: S3FileOperations) =>
    FileOperations.apply(disk, s3)
  }

  make[MediaTypeDetector].from { (cfg: StoragePluginConfig) =>
    new MediaTypeDetector(cfg.files.mediaTypeDetector)
  }

  make[LinkFileAction].from {
    (fetchStorage: FetchStorage, mediaTypeDetector: MediaTypeDetector, s3FileOps: S3FileOperations) =>
      LinkFileAction(fetchStorage, mediaTypeDetector, s3FileOps)
  }

  make[Files].from {
    (
        cfg: StoragePluginConfig,
        fetchContext: FetchContext,
        fetchStorage: FetchStorage,
        xas: Transactors,
        clock: Clock[IO],
        uuidF: UUIDF,
        as: ActorSystem,
        fileOps: FileOperations,
        mediaTypeDetector: MediaTypeDetector,
        linkFileAction: LinkFileAction
    ) =>
      Files(
        fetchContext,
        fetchStorage,
        FormDataExtractor(mediaTypeDetector)(as),
        xas,
        cfg.files.eventLog,
        fileOps,
        linkFileAction,
        clock
      )(uuidF)
  }

  make[FilesRoutes].from {
    (
        showLocation: ShowFileLocation,
        identities: Identities,
        aclCheck: AclCheck,
        files: Files,
        schemeDirectives: DeltaSchemeDirectives,
        indexingAction: AggregateIndexingAction,
        shift: File.Shift,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig
    ) =>
      new FilesRoutes(identities, aclCheck, files, schemeDirectives, indexingAction(_, _, _)(shift))(
        baseUri,
        showLocation,
        cr,
        ordering,
        fusionConfig
      )
  }

  make[LinkFilesRoutes].from {
    (
        showLocation: ShowFileLocation,
        identities: Identities,
        aclCheck: AclCheck,
        files: Files,
        indexingAction: AggregateIndexingAction,
        shift: File.Shift,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new LinkFilesRoutes(identities, aclCheck, files, indexingAction(_, _, _)(shift))(
        baseUri,
        cr,
        ordering,
        showLocation
      )
  }

  make[DelegateFilesRoutes].from {
    (
        identities: Identities,
        jwsPayloadHelper: JWSPayloadHelper,
        aclCheck: AclCheck,
        files: Files,
        indexingAction: AggregateIndexingAction,
        shift: File.Shift,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        showLocation: ShowFileLocation
    ) =>
      new DelegateFilesRoutes(
        identities,
        aclCheck,
        files,
        jwsPayloadHelper,
        indexingAction(_, _, _)(shift)
      )(baseUri, cr, ordering, showLocation)
  }

  make[File.Shift].from { (files: Files, base: BaseUri, showLocation: ShowFileLocation) =>
    File.shift(files)(base, showLocation)
  }

  many[ResourceShift[?, ?]].ref[File.Shift]

  many[ScopeInitialization].addSet { (storages: Storages, serviceAccount: ServiceAccount, cfg: StoragePluginConfig) =>
    Option.when(cfg.enableDefaultCreation)(StorageScopeInitialization(storages, serviceAccount, cfg.defaults)).toSet
  }

  many[ProjectDeletionTask].add { (storages: Storages) => StorageDeletionTask(storages) }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/storages-metadata.json"))

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/files.json"))

  many[RemoteContextResolution].addEffect {
    for {
      storageCtx     <- ContextValue.fromFile("contexts/storages.json")
      storageMetaCtx <- ContextValue.fromFile("contexts/storages-metadata.json")
      fileCtx        <- ContextValue.fromFile("contexts/files.json")
    } yield RemoteContextResolution.fixed(
      storageCtxId     -> storageCtx,
      storageMetaCtxId -> storageMetaCtx,
      fileCtxId        -> fileCtx
    )
  }

  many[ResourceToSchemaMappings].add(
    ResourceToSchemaMappings(Label.unsafe("files") -> filesSchemaId)
  )

  many[ApiMappings].add(Storages.mappings + Files.mappings)

  many[SseEncoder[?]].add { (base: BaseUri) => StorageEvent.sseEncoder(base) }
  many[SseEncoder[?]].add { (base: BaseUri, showLocation: ShowFileLocation) =>
    FileEvent.sseEncoder(base, showLocation)
  }

  many[ScopedEventMetricEncoder[?]].add { FileEvent.fileEventMetricEncoder }

  many[PriorityRoute].add { (storagesRoutes: StoragesRoutes) =>
    PriorityRoute(priority, storagesRoutes.routes, requiresStrictEntity = true)
  }
  many[PriorityRoute].add {
    (
        fileRoutes: FilesRoutes,
        linkFileRoutes: LinkFilesRoutes,
        delegationRoutes: DelegateFilesRoutes
    ) =>
      PriorityRoute(
        priority,
        concat(fileRoutes.routes, linkFileRoutes.routes, delegationRoutes.routes),
        requiresStrictEntity = false
      )
  }
}
