package ai.senscience.nexus.delta.rdf.utils

import java.nio.file.Path
import scala.annotation.tailrec

object PathUtils {

  /**
    * Checks if the ''target'' path is a descendant of the ''parent'' path. E.g.: path = /some/my/path ; parent = /some
    * will return true E.g.: path = /some/my/path ; parent = /other will return false
    */
  def descendantOf(target: Path, parent: Path): Boolean =
    inner(parent, target.getParent)

  @tailrec
  @SuppressWarnings(Array("NullParameter"))
  private def inner(parent: Path, child: Path): Boolean = {
    if child == null then false
    else if parent == child then true
    else inner(parent, child.getParent)
  }
}
