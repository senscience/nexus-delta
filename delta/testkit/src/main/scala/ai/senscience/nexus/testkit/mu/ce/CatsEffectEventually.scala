package ai.senscience.nexus.testkit.mu.ce

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig.MaximumCumulativeDelayConfig
import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategy}
import ai.senscience.nexus.testkit.mu.ce.CatsEffectEventually.logger
import cats.effect.IO
import munit.{Assertions, CatsEffectAssertions, Location}

trait CatsEffectEventually { self: Assertions with CatsEffectAssertions =>
  implicit class CatsEffectEventuallyOps[A](io: IO[A]) {
    def eventually(implicit loc: Location, patience: PatienceConfig): IO[A] = {
      val strategy = RetryStrategy[Throwable](
        MaximumCumulativeDelayConfig(patience.timeout, patience.interval),
        {
          case _: AssertionError => true
          case _                 => false
        },
        onError = (err, details) =>
          IO.whenA(details.givingUp) {
            logger.debug(err)(
              s"Giving up on ${err.getClass.getSimpleName}, ${details.retriesSoFar} retries after ${details.cumulativeDelay}."
            )
          }
      )
      io
        .retry(strategy)
        .adaptError { case e: AssertionError =>
          fail(s"assertion failed after retrying with eventually: ${e.getMessage}", e)
        }
    }
  }
}

object CatsEffectEventually {
  private val logger = Logger[CatsEffectEventually]
}
