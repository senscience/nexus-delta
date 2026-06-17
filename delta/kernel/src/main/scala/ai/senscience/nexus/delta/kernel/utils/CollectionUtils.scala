package ai.senscience.nexus.delta.kernel.utils

import fs2.Chunk

object CollectionUtils {

  /**
    * Displays all elements of this collection between quotes and separated by commas.
    * @param iterable
    *   the collection to display
    * @return
    *   a string representation of the colleciton
    * @example
    *   `CollectionUtils.quote(List(1, 2, 3)) = "'1','2','3'"`
    */
  def quote(iterable: Iterable[?]): String = iterable.mkString("'", "','", "'")

  def quote(chunk: Chunk[?]): String = quote(chunk.toList)

}
