package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.sourcing.offset.Offset
import ch.epfl.bluebrain.nexus.delta.kernel.utils.ClassUtils

object SSEUtils {

  /**
    * Take a couple of id and class, extracts the name of the class and generate the sequence according to the couple
    * position
    * @param values
    *   the values to transform
    */
  def extract[Id, Tpe](values: (Id, Tpe, Long)*): Seq[(Id, String, Offset)] =
    values.map { case (id, tpe, offset) =>
      (id, ClassUtils.simpleName(tpe), Offset.at(offset))
    }

}
