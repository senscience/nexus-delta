package ai.senscience.nexus.delta.plugins.storage

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.elasticsearch.metrics.MetricsIndexDef
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.plugins.storage.files.model.{File, FileEvent}
import ai.senscience.nexus.delta.plugins.storage.files.routes.{DelegateFilesRoutes, FilesRoutes, LinkFilesRoutes}
import ai.senscience.nexus.delta.plugins.storage.files.schemas.files as filesSchemaId
import ai.senscience.nexus.delta.plugins.storage.files.{contexts as fileContext, Files, FormDataExtractor, MediaTypeDetector}
import ai.senscience.nexus.delta.plugins.storage.storages.StoragesConfig.{ShowFileLocation, StorageTypeConfig}
import ai.senscience.nexus.delta.plugins.storage.storages.access.{S3StorageAccess, StorageAccess}
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageEvent
import ai.senscience.nexus.delta.plugins.storage.storages.operations.disk.DiskFileOperations
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.client.S3StorageClient
import ai.senscience.nexus.delta.plugins.storage.storages.operations.s3.{S3FileOperations, S3LocationGenerator}
import ai.senscience.nexus.delta.plugins.storage.storages.operations.{FileOperations, LinkFileAction}
import ai.senscience.nexus.delta.plugins.storage.storages.routes.StoragesRoutes
import ai.senscience.nexus.delta.plugins.storage.storages.{contexts as storageContext, *}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.indexing.SyncIndexingAction.AggregateIndexingAction
import ai.senscience.nexus.delta.sdk.indexing.MainDocumentEncoder
import ai.senscience.nexus.delta.sdk.jws.JWSPayloadHelper
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.metrics.ScopedEventMetricEncoder
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.server.Directives.concat
import org.http4s.Uri.Path
import org.typelevel.otel4s.trace.Tracer

/**
  * Storages and Files wiring
  */
class StoragePluginModule(priority: Int) extends NexusModuleDef {

  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[StoragePluginConfig]("plugins.storage")

  makeTracer("storages")

  makeTracer("files")

  addRemoteContextResolution(storageContext.definition ++ fileContext.definition)

  make[StorageTypeConfig].from { (cfg: StoragePluginConfig) => cfg.storages.storageTypeConfig }

  make[ShowFileLocation].from { (cfg: StorageTypeConfig) => cfg.showFileLocation }

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
          uuidF: UUIDF,
          tracer: Tracer[IO] @Id("storages")
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
        )(using uuidF)(using tracer)
    }

  make[FetchStorage].from { (storages: Storages, aclCheck: AclCheck) =>
    FetchStorage(storages, aclCheck)
  }

  make[StoragesStatistics].from {
    (
        client: ElasticSearchClient @Id("elasticsearch-query-client"),
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
        fusionConfig: FusionConfig,
        tracer: Tracer[IO] @Id("storages")
    ) =>
      {
        new StoragesRoutes(
          identities,
          aclCheck,
          storages,
          storagesStatistics,
          schemeDirectives
        )(using baseUri)(using cr, ordering, fusionConfig, tracer)
      }
  }

  make[DiskFileOperations].from { (uuidF: UUIDF) => DiskFileOperations.mk(using uuidF) }

  make[S3FileOperations].from { (client: S3StorageClient, locationGenerator: S3LocationGenerator, uuidF: UUIDF) =>
    S3FileOperations.mk(client, locationGenerator)(using uuidF)
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
        linkFileAction: LinkFileAction,
        tracer: Tracer[IO] @Id("files")
    ) =>
      Files(
        fetchContext,
        fetchStorage,
        FormDataExtractor(mediaTypeDetector)(using as),
        xas,
        cfg.files.eventLog,
        fileOps,
        linkFileAction,
        clock
      )(using uuidF, tracer)
  }

  make[FilesRoutes].from {
    (
        showLocation: ShowFileLocation,
        identities: Identities,
        aclCheck: AclCheck,
        files: Files,
        schemeDirectives: DeltaSchemeDirectives,
        indexingAction: AggregateIndexingAction,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig,
        tracer: Tracer[IO] @Id("files")
    ) =>
      new FilesRoutes(identities, aclCheck, files, schemeDirectives, indexingAction(Files.entityType)(_, _, _))(using
        baseUri
      )(using
        showLocation,
        cr,
        ordering,
        fusionConfig,
        tracer
      )
  }

  make[LinkFilesRoutes].from {
    (
        showLocation: ShowFileLocation,
        identities: Identities,
        aclCheck: AclCheck,
        files: Files,
        indexingAction: AggregateIndexingAction,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        tracer: Tracer[IO] @Id("files")
    ) =>
      new LinkFilesRoutes(
        identities,
        aclCheck,
        files,
        indexingAction(Files.entityType)(_, _, _)
      )(using baseUri)(using
        cr,
        ordering,
        showLocation,
        tracer
      )
  }

  make[DelegateFilesRoutes].from {
    (
        identities: Identities,
        jwsPayloadHelper: JWSPayloadHelper,
        aclCheck: AclCheck,
        files: Files,
        indexingAction: AggregateIndexingAction,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        showLocation: ShowFileLocation,
        tracer: Tracer[IO] @Id("files")
    ) =>
      new DelegateFilesRoutes(
        identities,
        aclCheck,
        files,
        jwsPayloadHelper,
        indexingAction(Files.entityType)(_, _, _)
      )(using baseUri)(using cr, ordering, showLocation, tracer)
  }

  addIndexingType(Files.entityType)

  many[ResourceShift[?, ?]].add { (files: Files, base: BaseUri, showLocation: ShowFileLocation) =>
    File.shift(files)(using base, showLocation)
  }

  many[MainDocumentEncoder[?, ?]].add { (base: BaseUri, showLocation: ShowFileLocation) =>
    File.mainDocumentEncoder(using base, showLocation)
  }

  many[ScopeInitialization].addSet {
    (
        storages: Storages,
        serviceAccount: ServiceAccount,
        cfg: StoragePluginConfig,
        tracer: Tracer[IO] @Id("storages")
    ) =>
      Option
        .when(cfg.enableDefaultCreation)(
          StorageScopeInitialization(storages, serviceAccount, cfg.defaults)(using tracer)
        )
        .toSet
  }

  many[ProjectDeletionTask].add { (storages: Storages) => StorageDeletionTask(storages) }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/files.json"))

  many[ResourceToSchemaMappings].add(
    ResourceToSchemaMappings(Label.unsafe("files") -> filesSchemaId)
  )

  many[ApiMappings].add(Storages.mappings + Files.mappings)

  many[SseEncoder[?]].add { (base: BaseUri) => StorageEvent.sseEncoder(using base) }
  many[SseEncoder[?]].add { (base: BaseUri, showLocation: ShowFileLocation) =>
    FileEvent.sseEncoder(using base, showLocation)
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
