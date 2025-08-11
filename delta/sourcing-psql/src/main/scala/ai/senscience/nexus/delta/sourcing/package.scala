package ai.senscience.nexus.delta

import doobie.enumerated.SqlState
import doobie.postgres.sqlstate

import java.sql.SQLException

package object sourcing {

  val UniqueConstraintViolation: SqlState = sqlstate.class23.UNIQUE_VIOLATION

  def isUniqueViolation(sql: SQLException): Boolean =
    sql.getSQLState == UniqueConstraintViolation.value

}
