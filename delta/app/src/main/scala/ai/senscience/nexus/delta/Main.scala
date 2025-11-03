package ai.senscience.nexus.delta

import ai.senscience.nexus.delta.config.{BuildInfo, ConfigLoader}
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugin.PluginsLoader.PluginLoaderConfig
import ai.senscience.nexus.delta.plugin.{PluginsLoader, WiringInitializer}
import ai.senscience.nexus.delta.sdk.error.PluginError.PluginInitializationError
import ai.senscience.nexus.delta.sdk.plugin.PluginDef
import ai.senscience.nexus.delta.sourcing.config.DatabaseConfig
import ai.senscience.nexus.delta.sourcing.stream.config.ProjectionConfig
import ai.senscience.nexus.delta.wiring.DeltaModule
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.typesafe.config.Config
import izumi.distage.model.Locator

object Main extends IOApp {

  private val externalConfigEnvVariable = "DELTA_EXTERNAL_CONF"
  private val pluginEnvVariable         = "DELTA_PLUGINS"
  private val logger                    = Logger[Main.type]
  val pluginsMaxPriority: Int           = 100
  val pluginsMinPriority: Int           = 1

  override def run(args: List[String]): IO[ExitCode] = {
    // TODO: disable this for now, but investigate why it happens
    System.setProperty("cats.effect.logNonDaemonThreadsOnExit", "false")
    // Enable Cats Effect fiber context tracking
    System.setProperty("cats.effect.trackFiberContext", "true")
    val config = sys.env.get(pluginEnvVariable).fold(PluginLoaderConfig())(PluginLoaderConfig(_))
    start(config)
      .use(_ => IO.never)
      .as(ExitCode.Success)
      .redeemWith(logTerminalError, IO.pure)
  }

  private def logTerminalError: Throwable => IO[ExitCode] = e =>
    logger.error(e)("Delta failed to start").as(ExitCode.Error)

  private[delta] def start(loaderConfig: PluginLoaderConfig): Resource[IO, Locator] =
    for {
      _                        <- Resource.eval(logger.info(s"Starting Nexus Delta version '${BuildInfo.version}'."))
      _                        <- Resource.eval(logger.info("Loading plugins and config..."))
      (config, cl, pluginDefs) <- Resource.eval(loadPluginsAndConfig(loaderConfig))
      modules                   = DeltaModule(config, runtime, cl)
      (plugins, locator)       <- WiringInitializer(modules, pluginDefs)
      _                        <- logDatabaseConfig(locator)
      _                        <- logClusterConfig(locator)
      _                        <- BootstrapPekko(locator, plugins)
    } yield locator

  private[delta] def loadPluginsAndConfig(
      config: PluginLoaderConfig
  ): IO[(Config, ClassLoader, List[PluginDef])] =
    for {
      (classLoader, pluginDefs) <- PluginsLoader(config).load
      _                         <- logPlugins(pluginDefs)
      enabledDefs                = pluginDefs.filter(_.enabled)
      _                         <- validatePriority(enabledDefs)
      _                         <- validateDifferentName(enabledDefs)
      configNames                = enabledDefs.map(_.configFileName)
      cfgPathOpt                 = sys.env.get(externalConfigEnvVariable)
      mergedConfig              <- ConfigLoader.loadOrThrow(cfgPathOpt, configNames, classLoader)
    } yield (mergedConfig, classLoader, enabledDefs)

  private def logDatabaseConfig(locator: Locator) = Resource.eval {
    val config = locator.get[DatabaseConfig]
    logger.info(s"Database partition strategy is ${config.partitionStrategy}") >>
      logger.info(s"Database config for reads is ${config.read.host} (${config.read.poolSize})") >>
      logger.info(s"Database config for writes is ${config.write.host} (${config.write.poolSize})") >>
      logger.info(s"Database config for streaming is ${config.streaming.host} (${config.streaming.poolSize})")
  }

  private def logClusterConfig(locator: Locator) = Resource.eval {
    val config = locator.get[ProjectionConfig].cluster
    if config.size == 1 then logger.info("Delta is running in standalone mode.")
    else
      logger.info(
        s"Delta is running in clustered mode. The current node is number ${config.nodeIndex} out of a total of ${config.size} nodes."
      )
  }

  private def logPlugins(pluginDefs: List[PluginDef]): IO[Unit] = {
    def pluginLogEntry(pdef: PluginDef): String =
      s"${pdef.info.name} - version: '${pdef.info.version}', enabled: '${pdef.enabled}'"

    if pluginDefs.isEmpty then logger.info("No plugins discovered.")
    else logger.info(s"Discovered plugins: ${pluginDefs.map(p => pluginLogEntry(p)).mkString("\n- ", "\n- ", "")}")
  }

  private def validatePriority(pluginsDef: List[PluginDef]): IO[Unit] =
    IO.raiseWhen(pluginsDef.map(_.priority).distinct.size != pluginsDef.size)(
      PluginInitializationError(
        "Several plugins have the same priority:" + pluginsDef
          .map(p => s"name '${p.info.name}' priority '${p.priority}'")
          .mkString(",")
      )
    ) >>
      (pluginsDef.find(p => p.priority > pluginsMaxPriority || p.priority < pluginsMinPriority) match {
        case Some(pluginDef) =>
          IO.raiseError(
            PluginInitializationError(
              s"Plugin '$pluginDef' has a priority out of the allowed range [$pluginsMinPriority - $pluginsMaxPriority]"
            )
          )
        case None            => IO.unit
      })

  private def validateDifferentName(pluginsDef: List[PluginDef]): IO[Unit] =
    IO.raiseWhen(pluginsDef.map(_.info.name).distinct.size != pluginsDef.size)(
      PluginInitializationError(
        s"Several plugins have the same name: ${pluginsDef.map(p => s"name '${p.info.name}'").mkString(",")}"
      )
    )
}
