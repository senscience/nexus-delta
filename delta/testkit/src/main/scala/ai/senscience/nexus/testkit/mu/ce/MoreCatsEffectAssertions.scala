package ai.senscience.nexus.testkit.mu.ce

import cats.effect.IO
import munit.{CatsEffectAssertions, Location}

import scala.reflect.ClassTag

trait MoreCatsEffectAssertions { self: CatsEffectAssertions =>
  extension [A](io: IO[A])(using Location) {
    def interceptEquals[E <: Throwable: ClassTag](expected: E): IO[Unit] = io.intercept[E].assertEquals(expected)
  }
}
