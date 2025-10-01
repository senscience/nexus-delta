package ai.senscience.nexus.delta.kernel

import cats.effect.IO
import org.typelevel.log4cats.Logger
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.*
import retry.retryingOnErrors
import retry.RetryDetails.NextStep.{DelayAndRetry, GiveUp}
import retry.RetryPolicies.*
import retry.ResultHandler.retryOnSomeErrors
import retry.{RetryDetails, RetryPolicies, RetryPolicy}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/**
  * Strategy to apply when an action fails
  * @param config
  *   the config which allows to define a cats-retry policy
  * @param retryWhen
  *   to decide whether a given error is worth retrying
  * @param onError
  *   an error handler
  */
final case class RetryStrategy(
    config: RetryStrategyConfig,
    retryWhen: Throwable => Boolean,
    onError: (Throwable, RetryDetails) => IO[Unit]
) {
  val policy: RetryPolicy[IO, Any] = config.toPolicy
}

object RetryStrategy {

  /**
    * Apply the provided strategy on the given io
    */
  def use[A](io: IO[A], retryStrategy: RetryStrategy): IO[A] =
    retryingOnErrors(io)(
      policy = retryStrategy.policy,
      errorHandler = retryOnSomeErrors(
        error => retryStrategy.retryWhen(error),
        (error, retryDetails) => retryStrategy.onError(error, retryDetails)
      )
    )

  /**
    * Log errors when retrying
    */
  def logError(logger: Logger[IO], action: String): (Throwable, RetryDetails) => IO[Unit] = {
    case (err, RetryDetails(retriesSoFar, cumulativeDelay, DelayAndRetry(nextDelay))) =>
      val message = s"""Error $err while $action: retrying in ${nextDelay.toMillis}ms (retries so far: $retriesSoFar)"""
      logger.warn(message)
    case (err, RetryDetails(retriesSoFar, cumulativeDelay, GiveUp))                   =>
      val message = s"""Error $err while $action, giving up (total retries: $retriesSoFar)"""
      logger.error(message)
  }

  def retryOnNonFatal(
      config: RetryStrategyConfig,
      logger: Logger[IO],
      action: String
  ): RetryStrategy =
    RetryStrategy(
      config,
      (t: Throwable) => NonFatal(t),
      (t: Throwable, d: RetryDetails) => logError(logger, action)(t, d)
    )

}

/**
  * Configuration for a [[RetryStrategy]]
  */
sealed trait RetryStrategyConfig extends Product with Serializable {
  def toPolicy: RetryPolicy[IO, Any]

}

object RetryStrategyConfig {

  /**
    * Fails without retry
    */
  case object AlwaysGiveUp extends RetryStrategyConfig {
    override def toPolicy: RetryPolicy[IO, Any] = alwaysGiveUp[IO[*]]
  }

  /**
    * Retry at a constant interval
    * @param delay
    *   the interval before a retry will be attempted
    * @param maxRetries
    *   the maximum number of retries
    */
  final case class ConstantStrategyConfig(delay: FiniteDuration, maxRetries: Int) extends RetryStrategyConfig {
    override def toPolicy: RetryPolicy[IO, Any] =
      constantDelay[IO[*]](delay).join(limitRetries(maxRetries))
  }

  /**
    * Retry exactly once
    * @param delay
    *   the interval before the retry will be attempted
    */
  final case class OnceStrategyConfig(delay: FiniteDuration) extends RetryStrategyConfig {
    override def toPolicy: RetryPolicy[IO, Any] =
      constantDelay[IO[*]](delay).join(limitRetries(1))
  }

  /**
    * Retry with an exponential delay after a failure
    * @param initialDelay
    *   the initial delay after the first failure
    * @param maxDelay
    *   the maximum delay to not exceed
    * @param maxRetries
    *   the maximum number of retries
    */
  final case class ExponentialStrategyConfig(initialDelay: FiniteDuration, maxDelay: FiniteDuration, maxRetries: Int)
      extends RetryStrategyConfig {
    override def toPolicy: RetryPolicy[IO, Any] =
      capDelay[IO, Any](maxDelay, fullJitter(initialDelay)).join(limitRetries(maxRetries))
  }

  /**
    * Retry with a constant delay until the total delay reaches the limit
    * @param threshold
    *   the maximum cumulative delay
    * @param delay
    *   the delay between each try
    */
  final case class MaximumCumulativeDelayConfig(threshold: FiniteDuration, delay: FiniteDuration)
      extends RetryStrategyConfig {
    override def toPolicy: RetryPolicy[IO, Any] =
      RetryPolicies.limitRetriesByCumulativeDelay(
        threshold,
        RetryPolicies.constantDelay(delay)
      )
  }

  implicit val retryStrategyConfigReader: ConfigReader[RetryStrategyConfig] = {
    val onceRetryStrategy: ConfigReader[OnceStrategyConfig]                        = deriveReader[OnceStrategyConfig]
    val constantRetryStrategy: ConfigReader[ConstantStrategyConfig]                = deriveReader[ConstantStrategyConfig]
    val exponentialRetryStrategy: ConfigReader[ExponentialStrategyConfig]          = deriveReader[ExponentialStrategyConfig]
    val maximumCumulativeDelayStrategy: ConfigReader[MaximumCumulativeDelayConfig] =
      deriveReader[MaximumCumulativeDelayConfig]

    ConfigReader.fromCursor { cursor =>
      for {
        obj      <- cursor.asObjectCursor
        rc       <- obj.atKey("retry")
        strategy <- ConfigReader[String].from(rc).flatMap {
                      case "never"         => Right(AlwaysGiveUp)
                      case "once"          => onceRetryStrategy.from(obj)
                      case "constant"      => constantRetryStrategy.from(obj)
                      case "exponential"   => exponentialRetryStrategy.from(obj)
                      case "maximum-delay" => maximumCumulativeDelayStrategy.from(obj)
                      case other           =>
                        val reason = CannotConvert(
                          other,
                          "string",
                          "'retry' value must be one of ('never', 'once', 'constant', 'exponential')"
                        )
                        obj.failed(reason)
                    }
      } yield strategy
    }
  }
}
