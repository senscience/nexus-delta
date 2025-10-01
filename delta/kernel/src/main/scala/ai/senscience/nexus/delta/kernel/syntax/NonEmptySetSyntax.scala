package ai.senscience.nexus.delta.kernel.syntax

import cats.data.NonEmptySet

trait NonEmptySetSyntax {
  implicit final def nonEmptySetSyntax[A](nes: NonEmptySet[A]): NonEmptySetOps[A] = new NonEmptySetOps[A](nes)
}

final class NonEmptySetOps[A](private val nes: NonEmptySet[A]) extends AnyVal {

  /**
    * Converts a NonEmptySet into a Set
    */
  def toSet: Set[A] =
    nes.foldLeft(Set.empty[A]) { case (s, a) => s + a }

}
