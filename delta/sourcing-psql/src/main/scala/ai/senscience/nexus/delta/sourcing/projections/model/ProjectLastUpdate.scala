package ai.senscience.nexus.delta.sourcing.projections.model

import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import doobie.Read
import doobie.postgres.implicits.*

import java.time.Instant

final case class ProjectLastUpdate(project: ProjectRef, lastInstant: Instant, lastOrdering: Offset)

object ProjectLastUpdate {

  implicit val projectLastUpdateRead: Read[ProjectLastUpdate] =
    Read[(Label, Label, Instant, Offset)].map { case (org, project, instant, offset) =>
      ProjectLastUpdate(ProjectRef(org, project), instant, offset)
    }

}
