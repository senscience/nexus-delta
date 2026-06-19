package ai.senscience.nexus.delta

import org.typelevel.doobie.enumerated.SqlState
import org.typelevel.doobie.postgres.sqlstate

import java.sql.SQLException

package object sourcing {

  val UniqueConstraintViolation: SqlState = sqlstate.class23.UNIQUE_VIOLATION

  def isUniqueViolation(sql: SQLException): Boolean =
    sql.getSQLState == UniqueConstraintViolation.value

}
