package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectLastUpdate
import ai.senscience.nexus.delta.sourcing.query.StreamingQuery
import cats.effect.IO
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import fs2.Stream

trait ProjectLastUpdateStream {

  /**
    * Stream updates from the database
    */
  def apply(offset: Offset): Stream[IO, ProjectLastUpdate]

  /**
    * Stream projects filtered by their last update time
    */
  def projects(timeRange: TimeRange): Stream[IO, ProjectRef]

}

object ProjectLastUpdateStream {

  def apply(xas: Transactors, config: QueryConfig): ProjectLastUpdateStream =
    new ProjectLastUpdateStream {

      override def apply(offset: Offset): Stream[IO, ProjectLastUpdate] =
        StreamingQuery[ProjectLastUpdate](
          offset,
          o => sql"""SELECT *
                |FROM project_last_updates
                |WHERE last_ordering > $o
                |ORDER BY last_ordering ASC
                |LIMIT ${config.batchSize}""".stripMargin.query[ProjectLastUpdate],
          _.lastOrdering,
          config.refreshStrategy,
          xas
        )

      override def projects(timeRange: TimeRange): Stream[IO, ProjectRef] = {
        val condition = timeRange match {
          case TimeRange.After(value)        => fr"WHERE last_instant > $value"
          case TimeRange.Before(value)       => fr"WHERE last_instant < $value"
          case TimeRange.Between(start, end) => fr"WHERE last_instant > $start AND last_instant < $end"
          case TimeRange.Anytime             => fr""
        }
        (sql"SELECT org, project FROM project_last_updates " ++ condition ++ fr" ORDER BY org, project")
          .query[(Label, Label)]
          .stream
          .map { case (org, project) => ProjectRef(org, project) }
          .transact(xas.read)
      }
    }

}
