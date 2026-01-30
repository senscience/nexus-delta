package ai.senscience.nexus.delta.rdf.syntax

import ai.senscience.nexus.delta.rdf.utils.IterableUtils

trait IterableSyntax {

  extension [A](iterable: Iterable[A]) {

    /**
      * @return
      *   Some(entry) where ''entry'' is the only available element on the sequence, Some(onEmpty) when the sequence has
      *   no elements, None otherwise
      */
    def singleEntryOr(onEmpty: => A): Option[A] =
      IterableUtils.singleEntryOr(iterable, onEmpty)

    /**
      * @return
      *   Some(entry) where ''entry'' is the only available element on the sequence, None otherwise
      */
    def singleEntry: Option[A] =
      IterableUtils.singleEntry(iterable)
  }
}
