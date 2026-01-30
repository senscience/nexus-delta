package ai.senscience.nexus.testkit.mu

import munit.{Assertions, Location}

import scala.reflect.ClassTag

trait EitherAssertions { self: Assertions =>

  extension [E, A](either: Either[E, A])(using Location) {

    def assertLeft(expected: E): Unit =
      either match {
        case Left(l)  => assertEquals(l, expected)
        case Right(r) => fail(s"Right caught: $r, expected as left: $expected")
      }

    def assertLeft(): Unit = {
      assert(either.isLeft, "Right caught, expected a left")
    }

    def assertLeftOf[T](using T: ClassTag[T]): Unit = {
      either match {
        case Left(T(_)) => ()
        case Left(l)    =>
          fail(s"Wrong left type caught, expected : '${T.runtimeClass.getName}', actual: '${l.getClass.getName}'")
        case Right(_)   => fail("Right caught, expected a left")
      }
    }

    def assertRight(expected: A): Unit =
      either match {
        case Left(l)  => fail(s"Left caught: $l, expected as right: $expected")
        case Right(r) => assertEquals(r, expected)
      }
  }

}
