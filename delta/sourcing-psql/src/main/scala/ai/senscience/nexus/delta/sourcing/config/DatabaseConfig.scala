package ai.senscience.nexus.delta.sourcing.config

import ai.senscience.nexus.delta.kernel.Secret
import ai.senscience.nexus.delta.sourcing.config.DatabaseConfig.{DatabaseAccess, OpentelemetryConfig}
import ai.senscience.nexus.delta.sourcing.partition.PartitionStrategy
import org.typelevel.doobie.otel4s.QueryCaptureConfig
import org.typelevel.doobie.otel4s.QueryCaptureConfig.{QueryParametersPolicy, QueryTextPolicy}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  * Database configuration
  * @param read
  *   Access to database for regular read access (fetch and listing operations)
  * @param write
  *   Access to database for write access
  * @param streaming
  *   Access to database for streaming access (indexing / SSEs)
  * @param partitionStrategy
  *   Partition strategy for the partitioned tables (scoped_events and scoped_states)
  * @param name
  *   The name of the database to connect to
  * @param username
  *   The database username
  * @param password
  *   The database password
  * @param tablesAutocreate
  *   When true it creates the tables on service boot
  * @param rewriteBatchInserts
  *   When true it creates the tables on service boot
  * @param slowQueryThreshold
  *   Threshold allowing to trigger a warning log when a query execution time reaches this limit
  */
final case class DatabaseConfig(
    read: DatabaseAccess,
    write: DatabaseAccess,
    streaming: DatabaseAccess,
    partitionStrategy: PartitionStrategy,
    name: String,
    username: String,
    password: Secret[String],
    tablesAutocreate: Boolean,
    rewriteBatchInserts: Boolean,
    slowQueryThreshold: FiniteDuration,
    otel: OpentelemetryConfig
)

object DatabaseConfig {

  given ConfigReader[DatabaseConfig] = {
    given ConfigReader[DatabaseAccess] = deriveReader[DatabaseAccess]
    deriveReader[DatabaseConfig]
  }

  final case class DatabaseAccess(host: String, port: Int, poolSize: Int)

  final case class OpentelemetryConfig(
      captureQueryText: QueryTextPolicy,
      captureQueryParameters: Boolean
  ) {

    /** The doobie-otel4s query-capture configuration derived from this config. */
    def queryCapture: QueryCaptureConfig =
      QueryCaptureConfig(
        captureQueryText,
        if captureQueryParameters then QueryParametersPolicy.NonBatchOnly else QueryParametersPolicy.None
      )
  }

  object OpentelemetryConfig {
    val default: OpentelemetryConfig =
      OpentelemetryConfig(QueryTextPolicy.ParameterizedOnly, captureQueryParameters = false)

    given ConfigReader[QueryTextPolicy] =
      ConfigReader.fromStringTry {
        case "none"          => Success(QueryTextPolicy.None)
        case "parameterized" => Success(QueryTextPolicy.ParameterizedOnly)
        case "always"        => Success(QueryTextPolicy.Always)
        case other           =>
          Failure(
            new IllegalArgumentException(
              s"Invalid 'capture-query-text' value '$other', expected one of: none, parameterized, always"
            )
          )
      }

    given ConfigReader[OpentelemetryConfig] = deriveReader[OpentelemetryConfig]
  }

}
