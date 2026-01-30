package ai.senscience.nexus.testkit.mu

import cats.effect.IO
import fs2.Stream
import munit.{Assertions, Location}

import scala.concurrent.duration.*

trait StreamAssertions extends Assertions {

  extension [A](stream: Stream[IO, A]) {

    private def compileToList(take: Long) = stream.take(take).timeout(3.seconds).mask.compile.toList

    def assertList(expected: List[A])(using Location): IO[Unit] =
      compileToList(expected.size.toLong).map { obtained =>
        assertEquals(obtained, expected, s"Got ${obtained.size} elements, ${expected.size} elements were expected.")
      }

    def assertSize(expected: Int)(using Location): IO[Unit] =
      compileToList(expected.toLong).map { obtained =>
        assertEquals(obtained.size, expected, s"Got ${obtained.size} elements, $expected elements were expected.")
      }

    def assert(expected: A*)(using Location): IO[Unit] = assertList(expected.toList)

    def assertEmpty(using Location): IO[Unit] = assertSize(0)

    def assertAll(take: Long, predicate: A => Boolean)(using Location): IO[Unit] =
      compileToList(take).map { obtained =>
        Assertions.assert(obtained.forall(predicate), "All the elements don't match the predicate.")
      }
  }

}
