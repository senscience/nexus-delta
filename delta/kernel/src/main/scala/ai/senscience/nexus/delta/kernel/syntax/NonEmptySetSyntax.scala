package ai.senscience.nexus.delta.kernel.syntax

import cats.data.NonEmptySet

trait NonEmptySetSyntax {

  extension [A](nes: NonEmptySet[A]) {

    /**
      * Converts a NonEmptySet into a Set
      */
    def toSet: Set[A] =
      nes.foldLeft(Set.empty[A]) { case (s, a) => s + a }
  }
}
