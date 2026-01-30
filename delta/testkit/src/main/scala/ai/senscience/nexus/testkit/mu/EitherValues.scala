package ai.senscience.nexus.testkit.mu

import munit.Assertions.fail
import munit.Suite
import munit.Location

trait EitherValues {
  self: Suite =>

  extension [L, R](either: Either[L, R]) {
    def rightValue(using Location): R = either match {
      case Right(value) => value
      case Left(value)  => fail(s"Expected Right but got Left($value)")
    }

    def leftValue(using Location): L = either match {
      case Left(value)  => value
      case Right(value) => fail(s"Expected Left but got Right($value)")
    }
  }
}
