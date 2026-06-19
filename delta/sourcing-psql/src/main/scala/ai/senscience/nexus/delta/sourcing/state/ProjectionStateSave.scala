package ai.senscience.nexus.delta.sourcing.state

import org.typelevel.doobie.ConnectionIO

/**
  * Allows to save states as a projection in another table
  */
final case class ProjectionStateSave[Id, S](insert: (Id, S) => ConnectionIO[Unit], delete: Id => ConnectionIO[Unit])

object ProjectionStateSave {

  def noop[Id, S]: ProjectionStateSave[Id, S] = ProjectionStateSave(
    (_, _) => org.typelevel.doobie.free.connection.unit,
    _ => org.typelevel.doobie.free.connection.unit
  )

}
