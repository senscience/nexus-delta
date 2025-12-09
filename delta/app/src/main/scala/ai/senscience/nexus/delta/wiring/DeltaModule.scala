package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main
import ai.senscience.nexus.delta.config.{DescriptionConfig, HttpConfig, StrictEntity}
import ai.senscience.nexus.delta.elasticsearch.ElasticSearchModule
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.PluginDescription
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.provisioning.ProvisioningCoordinator
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclProvisioning
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.indexing.SyncIndexingAction
import ai.senscience.nexus.delta.sdk.indexing.SyncIndexingAction.AggregateIndexingAction
import ai.senscience.nexus.delta.sdk.jws.{JWSConfig, JWSPayloadHelper}
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.plugin.PluginDef
import ai.senscience.nexus.delta.sdk.projects.ScopeInitializationErrorStore
import ai.senscience.nexus.delta.sdk.realms.RealmProvisioning
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.config.{DatabaseConfig, ElemQueryConfig, QueryConfig}
import ai.senscience.nexus.delta.sourcing.partition.DatabasePartitioner
import ai.senscience.nexus.delta.sourcing.stream.config.{ProjectLastUpdateConfig, ProjectionConfig}
import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{Clock, IO, Sync}
import com.typesafe.config.Config
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.oteljava.OtelJava

/**
  * Complete service wiring definitions.
  */
class DeltaModule(config: Config, runtime: IORuntime)(using ClassLoader) extends NexusModuleDef {
  addImplicit[Sync[IO]]
  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  make[Config].from(config)
  makeConfig[DescriptionConfig]("app.description")
  makeConfig[DatabaseConfig]("app.database")
  makeConfig[FusionConfig]("app.fusion")

  makeConfig[HttpConfig]("app.http")
  make[BaseUri].from { (http: HttpConfig) => http.baseUri }
  make[StrictEntity].from { (http: HttpConfig) => http.strictEntityTimeout }

  makeConfig[ProjectionConfig]("app.projections")
  make[QueryConfig].from { (config: ProjectionConfig) => config.query }

  makeConfig[ElemQueryConfig]("app.elem-query")
  makeConfig[ProjectLastUpdateConfig]("app.project-last-update")
  makeConfig[ServiceAccount]("app.service-account")

  make[Transactors].fromResource { (config: DatabaseConfig, otel: OtelJava[IO]) =>
    Transactors(config, Some(otel))
  }

  make[DatabasePartitioner].fromEffect { (config: DatabaseConfig, xas: Transactors) =>
    DatabasePartitioner(config.partitionStrategy, xas)
  }

  make[List[PluginDescription]].from { (pluginsDef: List[PluginDef]) => pluginsDef.map(_.info) }

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/metadata.json"))

  make[AggregateIndexingAction].from { (internal: Set[SyncIndexingAction]) =>
    AggregateIndexingAction(NonEmptyList.fromListUnsafe(internal.toList))
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
    RemoteContextResolution
      .loadResources(coreContexts)
      .map(_.merge(otherCtxResolutions.toSeq*))
  }

  make[Clock[IO]].from(implicitly[Clock[IO]])
  make[UUIDF].from(UUIDF.random)
  make[JsonKeyOrdering].from(
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )
  )

  makeConfig[JWSConfig]("app.jws")
  make[JWSPayloadHelper].from { (config: JWSConfig) =>
    JWSPayloadHelper(config)
  }

  make[ResourceShifts].from {
    (shifts: Set[ResourceShift[?, ?]], xas: Transactors, rcr: RemoteContextResolution @Id("aggregate")) =>
      ResourceShifts(shifts, xas)(rcr)
  }

  include(new PekkoModule())
  include(new OtelModule(runtime))
  include(PermissionsModule)
  include(AclsModule)
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
  include(ViewsCommonModule)
  include(VersionModule)
  include(EventsModule)
  include(ExportModule)
  include(StreamModule)
  include(SupervisionModule)
  include(TypeHierarchyModule)

}

object DeltaModule {

  final def apply(
      config: Config,
      runtime: IORuntime,
      classLoader: ClassLoader
  ): DeltaModule = new DeltaModule(config, runtime)(using classLoader)
}
