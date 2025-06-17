package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

trait ProjectsHealth {

  /**
    * Returns the list of unhealthy projects
    */
  def health: IO[Set[ProjectRef]]

}

object ProjectsHealth {

  def apply(errorStore: ScopeInitializationErrorStore): ProjectsHealth =
    new ProjectsHealth {
      override def health: IO[Set[ProjectRef]] =
        errorStore.fetch
          .map(_.map(row => ProjectRef(row.org, row.project)))
          .map(_.toSet)
    }

}
