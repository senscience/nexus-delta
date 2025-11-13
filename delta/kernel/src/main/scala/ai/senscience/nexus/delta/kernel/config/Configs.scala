package ai.senscience.nexus.delta.kernel.config

import ai.senscience.nexus.delta.kernel.Logger
import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import pureconfig.{ConfigReader, ConfigSource}

import java.io.{File, Reader}
import scala.reflect.ClassTag

object Configs {

  private val logger = Logger[Configs.type]

  private val parseOptions = ConfigParseOptions.defaults().setAllowMissing(false)

  private val resolverOptions = ConfigResolveOptions.defaults()

  /**
    * Loads the config from the file or return an empty configuration
    */
  def parseFile(file: Option[File]): IO[Config] =
    IO.blocking(file.fold(ConfigFactory.empty()) { f =>
      ConfigFactory.parseFile(f, parseOptions)
    })

  /**
    * Loads the config from resource
    */
  def parseResource(resource: String): IO[Config] =
    IO.blocking(ConfigFactory.parseResources(resource, parseOptions))

  /**
    * Loads the config from the reader
    */
  def parseReader(reader: Reader): IO[Config] =
    IO.blocking(ConfigFactory.parseReader(reader, parseOptions))

  def parseString(s: String): Config =
    ConfigFactory.parseString(s, parseOptions)

  /**
    * Merge the configs in order and load the namespace according to the config reader
    */
  def merge(configs: Config*): IO[Config] = IO.blocking {
    configs
      .foldLeft(ConfigFactory.defaultOverrides())(_.withFallback(_))
      .withFallback(ConfigFactory.load())
      .resolve(resolverOptions)
  }

  def load[A: ClassTag](config: Config, namespace: String)(using ConfigReader[A]): A =
    ConfigSource.fromConfig(config).at(namespace).loadOrThrow[A]

  def loadEffect[A: ClassTag](config: Config, namespace: String)(using ConfigReader[A]): IO[A] =
    IO.delay(load(config, namespace)).onError { case e =>
      logger.error(e)(s"Error when loading configuration at '$namespace'")
    }

}
