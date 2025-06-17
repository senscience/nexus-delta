package ai.senscience.nexus.delta.sourcing.config

import scala.concurrent.duration.FiniteDuration

case class PurgeConfig(deleteExpiredEvery: FiniteDuration, ttl: FiniteDuration)
