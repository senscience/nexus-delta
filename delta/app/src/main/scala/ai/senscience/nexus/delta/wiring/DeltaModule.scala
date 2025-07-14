package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main
import ai.senscience.nexus.delta.config.{AppConfig, StrictEntity}
import ai.senscience.nexus.delta.elasticsearch.ElasticSearchModule
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.provisioning.ProvisioningCoordinator
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.IndexingAction.AggregateIndexingAction
import ai.senscience.nexus.delta.sdk.acls.AclProvisioning
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.jws.JWSPayloadHelper
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.plugin.PluginDef
import ai.senscience.nexus.delta.sdk.projects.{ProjectsConfig, ScopeInitializationErrorStore}
import ai.senscience.nexus.delta.sdk.realms.RealmProvisioning
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.config.{DatabaseConfig, ElemQueryConfig, QueryConfig}
import ai.senscience.nexus.delta.sourcing.partition.DatabasePartitioner
import ai.senscience.nexus.delta.sourcing.stream.config.{ProjectLastUpdateConfig, ProjectionConfig}
import cats.data.NonEmptyList
import cats.effect.{Clock, IO, Sync}
import com.typesafe.config.Config
import izumi.distage.model.definition.{Id, ModuleDef}

/**
  * Complete service wiring definitions.
  *
  * @param appCfg
  *   the application configuration
  * @param config
  *   the raw merged and resolved configuration
  */
class DeltaModule(appCfg: AppConfig, config: Config)(implicit classLoader: ClassLoader) extends ModuleDef {

  addImplicit[Sync[IO]]
  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  make[AppConfig].from(appCfg)
  make[Config].from(config)
  make[DatabaseConfig].from(appCfg.database)
  make[FusionConfig].from { appCfg.fusion }
  make[ProjectsConfig].from { appCfg.projects }
  make[ProjectionConfig].from { appCfg.projections }
  make[ElemQueryConfig].from { appCfg.elemQuery }
  make[ProjectLastUpdateConfig].from { appCfg.projectLastUpdate }
  make[QueryConfig].from { appCfg.projections.query }
  make[BaseUri].from { appCfg.http.baseUri }
  make[StrictEntity].from { appCfg.http.strictEntityTimeout }
  make[ServiceAccount].from { appCfg.serviceAccount.value }

  make[Transactors].fromResource { () => Transactors(appCfg.database) }

  make[DatabasePartitioner].fromEffect { (xas: Transactors) =>
    DatabasePartitioner(appCfg.database.partitionStrategy, xas)
  }

  make[List[PluginDescription]].from { (pluginsDef: List[PluginDef]) => pluginsDef.map(_.info) }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/metadata.json"))

  make[AggregateIndexingAction].from {
    (
        internal: Set[IndexingAction],
        cr: RemoteContextResolution @Id("aggregate")
    ) =>
      AggregateIndexingAction(NonEmptyList.fromListUnsafe(internal.toList))(cr)
  }

  make[ScopeInitializationErrorStore].from { (xas: Transactors, clock: Clock[IO]) =>
    ScopeInitializationErrorStore(xas, clock)
  }

  make[ScopeInitializer].from {
    (
        inits: Set[ScopeInitialization],
        errorStore: ScopeInitializationErrorStore
    ) =>
      ScopeInitializer(inits, errorStore)
  }

  make[ProvisioningCoordinator].fromEffect { (realmProvisioning: RealmProvisioning, aclProvisioning: AclProvisioning) =>
    ProvisioningCoordinator(Vector(realmProvisioning, aclProvisioning))
  }

  make[RemoteContextResolution].named("aggregate").fromEffect { (otherCtxResolutions: Set[RemoteContextResolution]) =>
    for {
      errorCtx          <- ContextValue.fromFile("contexts/error.json")
      metadataCtx       <- ContextValue.fromFile("contexts/metadata.json")
      searchCtx         <- ContextValue.fromFile("contexts/search.json")
      pipelineCtx       <- ContextValue.fromFile("contexts/pipeline.json")
      remoteContextsCtx <- ContextValue.fromFile("contexts/remote-contexts.json")
      tagsCtx           <- ContextValue.fromFile("contexts/tags.json")
      versionCtx        <- ContextValue.fromFile("contexts/version.json")
      validationCtx     <- ContextValue.fromFile("contexts/validation.json")
    } yield RemoteContextResolution
      .fixed(
        contexts.error          -> errorCtx,
        contexts.metadata       -> metadataCtx,
        contexts.search         -> searchCtx,
        contexts.pipeline       -> pipelineCtx,
        contexts.remoteContexts -> remoteContextsCtx,
        contexts.tags           -> tagsCtx,
        contexts.version        -> versionCtx,
        contexts.validation     -> validationCtx
      )
      .merge(otherCtxResolutions.toSeq*)
  }

  make[Clock[IO]].from(implicitly[Clock[IO]])
  make[UUIDF].from(UUIDF.random)
  make[JsonKeyOrdering].from(
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )
  )

  make[JWSPayloadHelper].from { config: AppConfig =>
    JWSPayloadHelper(config.jws)
  }

  make[ResourceShifts].from {
    (shifts: Set[ResourceShift[?, ?]], xas: Transactors, rcr: RemoteContextResolution @Id("aggregate")) =>
      ResourceShifts(shifts, xas)(rcr)
  }

  include(new AkkaModule(appCfg, config))
  include(PermissionsModule)
  include(new AclsModule(appCfg.acls, appCfg.permissions))
  include(RealmsModule)
  include(OrganizationsModule)
  include(ProjectsModule)
  include(ResolversModule)
  include(SchemasModule)
  include(ResourcesModule)
  include(ResourcesTrialModule)
  include(MultiFetchModule)
  include(IdentitiesModule)
  include(new ElasticSearchModule(Main.pluginsMinPriority))
  include(VersionModule)
  include(EventsModule)
  include(ExportModule)
  include(StreamModule)
  include(SupervisionModule)
  include(TypeHierarchyModule)

}

object DeltaModule {

  /**
    * Complete service wiring definitions.
    *
    * @param appCfg
    *   the application configuration
    * @param config
    *   the raw merged and resolved configuration
    * @param classLoader
    *   the aggregated class loader
    */
  final def apply(
      appCfg: AppConfig,
      config: Config,
      classLoader: ClassLoader
  ): DeltaModule =
    new DeltaModule(appCfg, config)(classLoader)
}
