package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.cache.LocalCache
import ai.senscience.nexus.delta.sdk.projects.model.ProjectStatistics
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tag}
import cats.effect.IO
import doobie.postgres.implicits.*
import doobie.syntax.all.*

import java.time.Instant
import scala.concurrent.duration.*

trait ProjectsStatistics {

  /**
    * Retrieve the current counts (and latest instant) of events for the passed ''project''
    */
  def get(project: ProjectRef): IO[Option[ProjectStatistics]]
}

object ProjectsStatistics {

  def apply(xas: Transactors): IO[ProjectsStatistics] = {
    // TODO make the cache configurable
    LocalCache.apply[ProjectRef, ProjectStatistics](500, 3.seconds).map { cache => (project: ProjectRef) =>
      cache.getOrElseAttemptUpdate(
        project,
        sql"""
               | SELECT COUNT(id), SUM(rev), MAX(instant) FROM scoped_states
               | WHERE org = ${project.organization} and project = ${project.project} AND tag = ${Tag.Latest.value}
               | """.stripMargin
          .query[(Long, Option[Long], Option[Instant])]
          .unique
          .map {
            case (resources, Some(events), Some(instant)) => Some(ProjectStatistics(events, resources, instant))
            case (_, _, _)                                => None
          }
          .transact(xas.read)
      )
    }
  }
}
