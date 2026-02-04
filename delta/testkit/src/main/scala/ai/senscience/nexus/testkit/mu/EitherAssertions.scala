package ai.senscience.nexus.testkit.mu

import munit.{Assertions, Location}

import scala.reflect.ClassTag

trait EitherAssertions { self: Assertions =>

  extension [E, A](either: Either[E, A])(using Location) {

    def assertLeft(expected: E, clue: String = ""): Unit =
      either match {
        case Left(l)  => assertEquals(l, expected, clue)
        case Right(r) => fail(withClue(clue, s"Right caught: $r, expected as left: $expected"))
      }

    def assertLeft(): Unit = {
      assert(either.isLeft, "Right caught, expected a left")
    }

    def assertLeftOf[T](clue: String = "")(using T: ClassTag[T]): Unit = {
      either match {
        case Left(T(_)) => ()
        case Left(l)    =>
          val message = s"Wrong left type caught, expected: '${T.runtimeClass.getName}', actual: '${l.getClass.getName}'"
          fail(withClue(clue, message))
        case Right(_)   => fail(withClue(clue, "Right caught, expected a left"))
      }
    }

    def assertRight(expected: A, clue: String = ""): Unit =
      either match {
        case Left(l)  => fail(withClue(clue, s"Left caught: $l, expected as right: $expected"))
        case Right(r) => assertEquals(r, expected, clue)
      }
  }

  private def withClue(clue: String, message: String): String =
    if clue.isEmpty then message else s"$clue: $message"

}
