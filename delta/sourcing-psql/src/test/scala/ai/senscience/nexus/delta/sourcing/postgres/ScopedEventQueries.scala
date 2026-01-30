package ai.senscience.nexus.delta.sourcing.postgres

import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import cats.effect.IO
import doobie.syntax.all.*

object ScopedEventQueries {

  def distinctProjects(xas: Transactors): IO[Set[ProjectRef]] =
    sql"""SELECT distinct org, project from scoped_events"""
      .query[(Label, Label)]
      .map { case (org, proj) =>
        ProjectRef(org, proj)
      }
      .to[Set]
      .transact(xas.read)

}
