package ai.senscience.nexus.delta.sourcing.postgres

import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.IO
import doobie.syntax.all.*

object PartitionQueries {

  def partitionsOf(mainTable: String)(implicit xas: Transactors): IO[List[String]] = {
    val partitionPrefix = s"$mainTable%"
    sql"""|SELECT table_name from information_schema.tables
          |WHERE table_name != $mainTable
          |AND table_name LIKE $partitionPrefix
          |ORDER BY table_name""".stripMargin
      .query[String]
      .to[List]
      .transact(xas.read)
  }

}
