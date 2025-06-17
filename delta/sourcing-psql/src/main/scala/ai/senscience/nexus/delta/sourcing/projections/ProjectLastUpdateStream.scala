package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectLastUpdate
import ai.senscience.nexus.delta.sourcing.query.StreamingQuery
import cats.effect.IO
import doobie.syntax.all.*
import fs2.Stream

trait ProjectLastUpdateStream {

  /**
    * Stream updates from the database
    */
  def apply(offset: Offset): Stream[IO, ProjectLastUpdate]

}

object ProjectLastUpdateStream {

  def apply(xas: Transactors, config: QueryConfig): ProjectLastUpdateStream =
    (offset: Offset) =>
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

}
