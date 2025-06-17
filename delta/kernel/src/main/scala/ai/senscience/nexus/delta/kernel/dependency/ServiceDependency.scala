package ai.senscience.nexus.delta.kernel.dependency

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import cats.effect.IO

/**
  * A description of a service that is used by the system
  */
trait ServiceDependency {

  /**
    * @return
    *   the service description of the current dependency
    */
  def serviceDescription: IO[ServiceDescription]
}
