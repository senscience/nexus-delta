package ai.senscience.nexus.testkit.scalatest.ce

import cats.effect.IO
import cats.effect.unsafe.implicits.*
import org.scalactic.source
import org.scalatest.{Assertion, Assertions}

import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag

trait CatsIOValues { self: Assertions =>

  extension [A](io: IO[A]) {
    def accepted(using pos: source.Position): A = {
      io.unsafeRunTimed(45.seconds).getOrElse(fail(s"IO timed out during .accepted call at $pos"))
    }

    def rejected(using pos: source.Position): Throwable = rejectedWith[Throwable]

    def assertRejectedEquals[E](expected: E)(using pos: source.Position, EE: ClassTag[E]): Assertion =
      assertResult(expected)(rejectedWith[E])

    def assertRejectedWith[E](using pos: source.Position, EE: ClassTag[E]): Assertion = {
      rejectedWith[E]
      succeed
    }

    def rejectedWith[E](using pos: source.Position, EE: ClassTag[E]): E = {
      io.attempt.accepted match {
        case Left(EE(value)) => value
        case Left(value)     =>
          fail(
            s"Wrong raised error type caught, expected: '${EE.runtimeClass.getName}', actual: '${value.getClass.getName}' at $pos"
          )
        case Right(value)    =>
          fail(
            s"Expected raising error, but returned successful response with type '${value.getClass.getName}' at $pos"
          )
      }
    }
  }
}
