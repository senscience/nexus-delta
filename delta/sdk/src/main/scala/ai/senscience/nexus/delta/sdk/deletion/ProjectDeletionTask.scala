package ai.senscience.nexus.delta.sdk.deletion

import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

/**
  * Task to be completed during project deletion
  */
trait ProjectDeletionTask {

  /**
    * Perform the deletion task for the given project on behalf of the given user
    */
  def apply(project: ProjectRef)(using Subject): IO[ProjectDeletionReport.Stage]

}
