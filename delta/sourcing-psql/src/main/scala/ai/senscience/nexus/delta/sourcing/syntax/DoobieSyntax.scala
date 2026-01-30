package ai.senscience.nexus.delta.sourcing.syntax

import ai.senscience.nexus.delta.sourcing.FragmentEncoder
import doobie.util.fragment.Fragment

/**
  * This package provides syntax via enrichment classes for Doobie
  */
trait DoobieSyntax {

  extension [A](value: A) {
    def asFragment(using encoder: FragmentEncoder[A]): Option[Fragment] = encoder(value)
  }
}
