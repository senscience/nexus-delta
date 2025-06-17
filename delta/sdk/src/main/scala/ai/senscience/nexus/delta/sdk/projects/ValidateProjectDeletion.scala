package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectDeletionIsDisabled, ProjectIsReferenced}
import ai.senscience.nexus.delta.sourcing.model.EntityDependency.ReferencedBy
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.{EntityDependencyStore, Transactors}
import cats.effect.IO

/**
  * Validate if a project can be deleted
  */
trait ValidateProjectDeletion {

  def apply(project: ProjectRef): IO[Unit]
}

object ValidateProjectDeletion {

  def apply(xas: Transactors, enabled: Boolean): ValidateProjectDeletion =
    apply(EntityDependencyStore.directExternalReferences(_, xas), enabled)

  def apply(fetchReferences: ProjectRef => IO[Set[ReferencedBy]], enabled: Boolean): ValidateProjectDeletion =
    (project: ProjectRef) =>
      IO.raiseWhen(!enabled)(ProjectDeletionIsDisabled) >>
        fetchReferences(project).flatMap { references =>
          IO.raiseWhen(references.nonEmpty)(ProjectIsReferenced(project, references))
        }

}
