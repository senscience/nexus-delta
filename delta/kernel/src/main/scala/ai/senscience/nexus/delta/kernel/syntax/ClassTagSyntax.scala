package ai.senscience.nexus.delta.kernel.syntax

import scala.reflect.ClassTag

trait ClassTagSyntax {

  /**
    * @return
    *   the simple name of the class from the implicitly available [[ClassTag]] instance.
    */
  extension [A](classTag: ClassTag[A]) {
    def simpleName: String = classTag.runtimeClass.getSimpleName
  }
}
