package ai.senscience.nexus.testkit.mu.ce

import cats.effect.IO
import cats.effect.unsafe.implicits.*
import munit.{Assertions, Location}

import scala.concurrent.duration.DurationInt

trait CatsIOValues { self: Assertions =>

  extension [A](io: IO[A]) {

    private def attemptRun =
      io.attempt
        .unsafeRunTimed(45.seconds)
        .getOrElse(
          fail("IO timed out during .accepted/.failed call")
        )

    def accepted(using Location): A =
      attemptRun match {
        case Left(error)  => fail(s"IO failed with error '$error'")
        case Right(value) => value
      }

    def failed(using Location): Throwable =
      attemptRun match {
        case Left(error)  => error
        case Right(value) => fail(s"IO succeeded with value '$value'")
      }
  }
}
