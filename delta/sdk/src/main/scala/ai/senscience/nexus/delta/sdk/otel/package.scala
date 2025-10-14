package ai.senscience.nexus.delta.sdk

import scala.concurrent.duration.FiniteDuration

package object otel {

  def secondsFromDuration(duration: FiniteDuration): Double =
    duration.toMillis / 1000.0

}
