package ai.senscience.nexus.delta.config

import ai.senscience.nexus.delta.kernel.config.Configs
import cats.effect.IO
import cats.syntax.all.*
import com.typesafe.config.Config

import java.io.{File, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8

object ConfigLoader {

  /**
    * Loads the application in two steps, wrapping the error type:
    *
    *   1. loads the default.conf and identifies the database configuration
    *   2. reloads the config using the selected database configuration and the plugin configurations
    */
  def loadOrThrow(
      externalConfigPath: Option[String] = None,
      pluginsConfigPaths: List[String] = List.empty,
      accClassLoader: ClassLoader = getClass.getClassLoader
  ): IO[Config] =
    load(externalConfigPath, pluginsConfigPaths, accClassLoader)

  /**
    * Loads the application in two steps:
    *
    *   1. loads default.conf and identifies the database configuration
    *   2. reloads the config using the selected database configuration and the plugin configurations
    */
  def load(
      externalConfigPath: Option[String] = None,
      pluginsConfigPaths: List[String] = List.empty,
      accClassLoader: ClassLoader = getClass.getClassLoader
  ): IO[Config] = {
    for {
      externalConfig <- Configs.parseFile(externalConfigPath.map(new File(_)))
      defaultConfig  <- Configs.parseResource("default.conf")
      pluginConfigs  <- pluginsConfigPaths.traverse { path =>
                          Configs.parseReader(new InputStreamReader(accClassLoader.getResourceAsStream(path), UTF_8))
                        }
      mergedConfig   <- Configs.merge(externalConfig :: defaultConfig :: pluginConfigs: _*)
    } yield mergedConfig
  }
}
