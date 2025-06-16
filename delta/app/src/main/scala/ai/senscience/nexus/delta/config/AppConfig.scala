package ai.senscience.nexus.delta.config

import ai.senscience.nexus.delta.sdk.acls.AclsConfig
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.jws.JWSConfig
import ai.senscience.nexus.delta.sdk.model.ServiceAccountConfig
import ai.senscience.nexus.delta.sdk.organizations.OrganizationsConfig
import ai.senscience.nexus.delta.sdk.permissions.PermissionsConfig
import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig
import ai.senscience.nexus.delta.sdk.realms.RealmsConfig
import ai.senscience.nexus.delta.sdk.resolvers.ResolversConfig
import ai.senscience.nexus.delta.sdk.resources.ResourcesConfig
import ai.senscience.nexus.delta.sdk.schemas.SchemasConfig
import ai.senscience.nexus.delta.sdk.sse.SseConfig
import ai.senscience.nexus.delta.sdk.typehierarchy.TypeHierarchyConfig
import cats.effect.IO
import cats.syntax.all.*
import ch.epfl.bluebrain.nexus.delta.kernel.cache.CacheConfig
import ch.epfl.bluebrain.nexus.delta.kernel.config.Configs
import ch.epfl.bluebrain.nexus.delta.sourcing.config.{DatabaseConfig, ElemQueryConfig}
import ch.epfl.bluebrain.nexus.delta.sourcing.exporter.ExportConfig
import ch.epfl.bluebrain.nexus.delta.sourcing.stream.config.{ProjectLastUpdateConfig, ProjectionConfig}
import com.typesafe.config.Config
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import java.io.{File, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8

/**
  * Main application configuration.
  */
final case class AppConfig(
    description: DescriptionConfig,
    http: HttpConfig,
    database: DatabaseConfig,
    identities: CacheConfig,
    permissions: PermissionsConfig,
    realms: RealmsConfig,
    organizations: OrganizationsConfig,
    acls: AclsConfig,
    projects: ProjectsConfig,
    resolvers: ResolversConfig,
    resources: ResourcesConfig,
    schemas: SchemasConfig,
    typeHierarchy: TypeHierarchyConfig,
    serviceAccount: ServiceAccountConfig,
    elemQuery: ElemQueryConfig,
    sse: SseConfig,
    projections: ProjectionConfig,
    projectLastUpdate: ProjectLastUpdateConfig,
    fusion: FusionConfig,
    `export`: ExportConfig,
    jws: JWSConfig
)

object AppConfig {

  /**
    * Loads the application in two steps, wrapping the error type:
    *
    *   1. loads the default default.conf and identifies the database configuration
    *   2. reloads the config using the selected database configuration and the plugin configurations
    */
  def loadOrThrow(
      externalConfigPath: Option[String] = None,
      pluginsConfigPaths: List[String] = List.empty,
      accClassLoader: ClassLoader = getClass.getClassLoader
  ): IO[(AppConfig, Config)] =
    load(externalConfigPath, pluginsConfigPaths, accClassLoader)

  /**
    * Loads the application in two steps:
    *
    *   1. loads the default default.conf and identifies the database configuration
    *   2. reloads the config using the selected database configuration and the plugin configurations
    */
  def load(
      externalConfigPath: Option[String] = None,
      pluginsConfigPaths: List[String] = List.empty,
      accClassLoader: ClassLoader = getClass.getClassLoader
  ): IO[(AppConfig, Config)] = {
    for {
      externalConfig            <- Configs.parseFile(externalConfigPath.map(new File(_)))
      defaultConfig             <- Configs.parseResource("default.conf")
      pluginConfigs             <- pluginsConfigPaths.traverse { path =>
                                     Configs.parseReader(new InputStreamReader(accClassLoader.getResourceAsStream(path), UTF_8))
                                   }
      (appConfig, mergedConfig) <- Configs.merge[AppConfig]("app", externalConfig :: defaultConfig :: pluginConfigs: _*)
    } yield (appConfig, mergedConfig)
  }

  implicit final val appConfigReader: ConfigReader[AppConfig] =
    deriveReader[AppConfig]
}
