package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.event.Event
import ai.senscience.nexus.delta.sourcing.state.State
import cats.effect.IO
import doobie.util.log
import doobie.util.log.{LogEvent, LogHandler, Parameters}
import io.circe.Json

import java.sql.SQLException
import scala.concurrent.duration.FiniteDuration

object QueryLogHandler {

  private val logger = Logger[QueryLogHandler.type]

  def apply(poolName: String, slowQueryThreshold: FiniteDuration): LogHandler[IO] = new LogHandler[IO] {
    override def run(logEvent: LogEvent): IO[Unit] = logEvent match {
      case log.Success(sql, params, label, exec, processing) if exec > slowQueryThreshold                 =>
        logger.warn(s"""[$poolName] Slow Statement Execution:
             |
             | ${formatQuery(sql)}
             |
             | arguments = ${formatArguments(params)}
             | label     = $label
             | elapsed = ${exec.toMillis} ms exec + ${processing.toMillis} ms processing (${(exec + processing).toMillis} ms total)
          """.stripMargin)
      case log.Success(sql, params, label, exec, processing)                                              =>
        logger.debug(s"""[$poolName] Successful Statement Execution:
             |
             | ${formatQuery(sql)}
             |
             | arguments = ${formatArguments(params)}
             | label     = $label
             | elapsed = ${exec.toMillis} ms exec + ${processing.toMillis} ms processing (${(exec + processing).toMillis} ms total)
          """.stripMargin)
      case log.ProcessingFailure(sql, params, label, exec, processing, failure)                           =>
        logger.error(failure)(s"""[$poolName] Failed Resultset Processing:
             |
             | ${formatQuery(sql)}
             |
             | arguments = ${formatArguments(params)}
             | label     = $label
             | elapsed = ${exec.toMillis} ms exec + ${processing.toMillis} ms processing (failed) (${(exec + processing).toMillis.toString} ms total)
          """.stripMargin)
      case log.ExecFailure(sql, params, label, exec, failure: SQLException) if isUniqueViolation(failure) =>
        logger.warn(failure)(s"""[$poolName] A unique constraint violation occured while running query:
                                 |
                                 | ${formatQuery(sql)}
                                 |
                                 | arguments = ${formatArguments(params)}
                                 | label     = $label
                                 | elapsed = ${exec.toMillis} ms exec (failed)
          """.stripMargin)
      case log.ExecFailure(sql, params, label, exec, failure)                                             =>
        logger.error(failure)(s"""[$poolName] Failed Statement Execution:
             |
             | ${formatQuery(sql)}
             |
             | arguments = ${formatArguments(params)}
             | label     = $label
             | elapsed = ${exec.toMillis} ms exec (failed)
          """.stripMargin)
    }

    private def formatQuery(sql: String) = sql.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")

    private def formatArguments(params: Parameters) = params.allParams.flatten
      .map {
        case _: Json  => "{json blob}"
        case e: Event => s"{event ${e.getClass.getSimpleName}}"
        case s: State => s"{state ${s.getClass.getSimpleName}}"
        case other    => other.toString
      }
      .mkString("[", ", ", "]")
  }

}
