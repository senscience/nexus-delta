package ai.senscience.nexus.delta.dependency

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import ai.senscience.nexus.delta.kernel.dependency.ServiceDependency
import ai.senscience.nexus.delta.sourcing.Transactors
import cats.effect.IO
import doobie.syntax.all.*

import scala.concurrent.duration.*

/**
  * Describes the postgres [[ServiceDependency]] providing a way to extract the [[ServiceDescription]] from a ''select
  * version();'' SQL command
  */
class PostgresServiceDependency(xas: Transactors) extends ServiceDependency {

  private val regex                                       = "( ?PostgreSQL )([^ ]+)(.*)".r
  private val serviceName                                 = "postgres"
  override def serviceDescription: IO[ServiceDescription] =
    sql"select version()"
      .query[String]
      .to[List]
      .transact(xas.read)
      .timeout(1.second)
      .map {
        case versionString :: _ =>
          versionString match {
            case regex(_, version, _) => ServiceDescription(serviceName, version)
            case _                    => ServiceDescription(serviceName, versionString)
          }
        case Nil                => ServiceDescription.unresolved(serviceName)
      }
      .handleError(_ => ServiceDescription.unresolved(serviceName))
}
