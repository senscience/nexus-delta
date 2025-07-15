package ai.senscience.nexus.delta.config

import ai.senscience.nexus.delta.kernel.config.Configs
import ai.senscience.nexus.testkit.mu.NexusSuite
import com.typesafe.config.Config
import com.typesafe.config.impl.ConfigImpl

class ConfigLoaderSuite extends NexusSuite {

  private def clearProperties(): Unit = {
    System.clearProperty("app.description.name")
    ConfigImpl.reloadSystemPropertiesConfig()
  }

  override def beforeEach(context: BeforeEach): Unit = clearProperties()

  override def afterEach(context: AfterEach): Unit = clearProperties()

  private val externalConfigPath = loader.absolutePath("config/external.conf").accepted

  private def getDescriptionName(config: Config) =
    Configs.load[DescriptionConfig](config, "app.description").name.value

  test("Load configuration without override") {
    ConfigLoader
      .load()
      .map(getDescriptionName)
      .assertEquals("delta")
  }

  test("Load configuration overridden with an external file") {
    ConfigLoader
      .load(externalConfigPath = Some(externalConfigPath))
      .map(getDescriptionName)
      .assertEquals("override name by file")
  }

  test("Load app name as system properties have a higher priority than the external config file") {
    val expected = "override name by property"
    System.setProperty("app.description.name", expected)
    ConfigImpl.reloadSystemPropertiesConfig()
    ConfigLoader
      .load(externalConfigPath = Some(externalConfigPath))
      .map(getDescriptionName)
      .assertEquals(expected)
  }

}
