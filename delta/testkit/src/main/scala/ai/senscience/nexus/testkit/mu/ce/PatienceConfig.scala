package ai.senscience.nexus.testkit.mu.ce

import scala.concurrent.duration.FiniteDuration

final case class PatienceConfig(timeout: FiniteDuration, interval: FiniteDuration)
