package ai.senscience.nexus.delta.sourcing.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration of an ephemeral log
  * @param maxDuration
  *   maximum duration for the evaluation of a command
  * @param ttl
  *   life span of created ephemeral states
  */
final case class EphemeralLogConfig(maxDuration: FiniteDuration, ttl: FiniteDuration)

object EphemeralLogConfig {
  given ConfigReader[EphemeralLogConfig] = deriveReader[EphemeralLogConfig]
}
