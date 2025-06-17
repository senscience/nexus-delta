package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.sdk.ScopeInitializer
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

trait ProjectHealer {

  /**
    * Heal the project
    */
  def heal(project: ProjectRef): IO[Unit]

}

object ProjectHealer {

  def apply(
      errorStore: ScopeInitializationErrorStore,
      scopeInitializer: ScopeInitializer,
      serviceAccount: ServiceAccount
  ): ProjectHealer =
    new ProjectHealer {
      implicit private val serviceAccountSubject: Subject = serviceAccount.subject

      override def heal(project: ProjectRef): IO[Unit] =
        scopeInitializer.initializeProject(project) >> errorStore.delete(project)

    }

}
