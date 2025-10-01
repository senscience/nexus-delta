package ai.senscience.nexus.delta.kernel

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig.{AlwaysGiveUp, ConstantStrategyConfig, ExponentialStrategyConfig, MaximumCumulativeDelayConfig, OnceStrategyConfig}
import ai.senscience.nexus.delta.kernel.config.Configs
import munit.{FunSuite, Location}
import pureconfig.error.ConfigReaderException

import scala.concurrent.duration.DurationInt

class RetryStrategyConfigSuite extends FunSuite {

  private def assertSuccess(config: String, expected: RetryStrategyConfig)(implicit location: Location): Unit = {
    val result = Configs.load[RetryStrategyConfig](Configs.parseString(config), "retry-config")
    assertEquals(result, expected)
  }

  private def assertFail(config: String)(implicit location: Location): Unit = {
    intercept[ConfigReaderException[RetryStrategyConfig]] {
      Configs.load[RetryStrategyConfig](Configs.parseString(config), "retry-config")
    }
    ()
  }

  test("Parse a always give up strategy") {
    val config = "retry-config { retry = never }"
    assertSuccess(config, AlwaysGiveUp)
  }

  test("Parse a constant retry strategy") {
    val config = "retry-config { retry = constant, delay = 15s , max-retries = 5 }"
    assertSuccess(config, ConstantStrategyConfig(15.seconds, 5))
  }

  test("Parse a once retry strategy") {
    val config = "retry-config { retry = once, delay = 15s }"
    assertSuccess(config, OnceStrategyConfig(15.seconds))
  }

  test("Parse a exponential retry strategy") {
    val config = "retry-config { retry = exponential, initial-delay = 15s, max-delay = 30s , max-retries = 5 }"
    assertSuccess(config, ExponentialStrategyConfig(15.seconds, 30.seconds, 5))
  }

  test("Parse a maximum-cumulative retry strategy") {
    val config = "retry-config { retry = maximum-delay, threshold = 30s, delay = 3s }"
    assertSuccess(config, MaximumCumulativeDelayConfig(30.seconds, 3.seconds))
  }

  test("Parse a maximum-cumulative retry strategy") {
    val config = "retry-config { retry = maximum-delay, threshold = 30s, delay = 3s }"
    assertSuccess(config, MaximumCumulativeDelayConfig(30.seconds, 3.seconds))
  }

  test("Fail for an unknown strategy") {
    val config = "retry-config { retry = unknown, x = a }"
    assertFail(config)
  }

}
