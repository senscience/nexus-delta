package ai.senscience.nexus.delta.sdk.permissions

import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import cats.implicits.*
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.*

/**
  * The permissions module config.
  * @param eventLog
  *   The event log configuration
  * @param minimum
  *   the minimum collection of permissions
  */
final case class PermissionsConfig(
    eventLog: EventLogConfig,
    minimum: Set[Permission],
    ownerPermissions: Set[Permission]
)

object PermissionsConfig {

  given ConfigReader[Permission] =
    ConfigReader.fromString(str =>
      Permission(str).leftMap(err => CannotConvert(str, classOf[Permission].getSimpleName, err.getMessage))
    )

  given ConfigReader[PermissionsConfig] = deriveReader[PermissionsConfig]

}
