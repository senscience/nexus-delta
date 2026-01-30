package ai.senscience.nexus.testkit.scalatest

import org.scalactic.source
import org.scalatest.Suite
import org.scalatest.exceptions.{StackDepthException, TestFailedException}

trait EitherValues { self: Suite =>

  extension [L, R](either: Either[L, R]) {
    def rightValue(using pos: source.Position): R =
      either match {
        case Right(value) => value
        case Left(_)      =>
          throw new TestFailedException(
            (_: StackDepthException) => Some("The Either value is not a Right(_)"),
            None,
            pos
          )
      }

    def leftValue(using pos: source.Position): L =
      either match {
        case Left(value) => value
        case Right(_)    =>
          throw new TestFailedException(
            (_: StackDepthException) => Some("The Either value is not a Left(_)"),
            None,
            pos
          )
      }
  }
}
