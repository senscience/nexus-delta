package ai.senscience.nexus.testkit.scalatest.ce

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig.MaximumCumulativeDelayConfig
import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategy}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectEventually.logger
import cats.effect.IO
import org.scalactic.source.Position
import org.scalatest.Assertions
import org.scalatest.enablers.Retrying
import org.scalatest.exceptions.TestFailedException
import org.scalatest.time.Span
import retry.RetryDetails.NextStep

trait CatsEffectEventually { self: Assertions =>
  given ioRetrying: [T] => Retrying[IO[T]] = new Retrying[IO[T]] {
    override def retry(timeout: Span, interval: Span, pos: Position)(fun: => IO[T]): IO[T] = {
      val strategy = RetryStrategy(
        MaximumCumulativeDelayConfig(timeout, interval),
        {
          case _: TestFailedException => true
          case _                      => false
        },
        onError = (err, details) =>
          IO.whenA(details.nextStepIfUnsuccessful == NextStep.GiveUp) {
            logger.error(err)(
              s"Giving up on ${err.getClass.getSimpleName}, ${details.retriesSoFar} retries after ${details.cumulativeDelay}."
            )
          }
      )
      RetryStrategy.use(fun, strategy).adaptError { case e: AssertionError =>
        fail(s"Assertion failed after retrying with eventually: ${e.getMessage}", e)
      }
    }
  }
}

object CatsEffectEventually {
  private val logger = Logger[CatsEffectEventually]
}
