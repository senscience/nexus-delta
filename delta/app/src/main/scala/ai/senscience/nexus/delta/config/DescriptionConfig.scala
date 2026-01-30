package ai.senscience.nexus.delta.config

import ai.senscience.nexus.delta.sdk.model.Name
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * The service description.
  *
  * @param name
  *   the name of the service
  * @param env
  *   the name of the environment in which the service runs
  */
final case class DescriptionConfig(name: Name, env: Name) {

  /**
    * @return
    *   the version of the service
    */
  val version: String = BuildInfo.version

  /**
    * @return
    *   the full name of the service (name + version)
    */
  val fullName: String = s"$name-${version.replaceAll("\\W", "-")}"
}

object DescriptionConfig {

  given ConfigReader[DescriptionConfig] = deriveReader[DescriptionConfig]
}
