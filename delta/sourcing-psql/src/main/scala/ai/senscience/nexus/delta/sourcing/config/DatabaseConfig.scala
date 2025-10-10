package ai.senscience.nexus.delta.sourcing.config

import ai.senscience.nexus.delta.kernel.Secret
import ai.senscience.nexus.delta.sourcing.config.DatabaseConfig.{DatabaseAccess, OpentelemetryConfig}
import ai.senscience.nexus.delta.sourcing.partition.PartitionStrategy
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

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
    given ConfigReader[DatabaseAccess]      = deriveReader[DatabaseAccess]
    given ConfigReader[OpentelemetryConfig] = deriveReader[OpentelemetryConfig]
    deriveReader[DatabaseConfig]
  }

  final case class DatabaseAccess(host: String, port: Int, poolSize: Int)

  final case class OpentelemetryConfig(
      statementInstrumenterEnabled: Boolean,
      statementSanitizationEnabled: Boolean,
      captureQueryParameters: Boolean,
      transactionInstrumenterEnabled: Boolean
  )

  object OpentelemetryConfig {
    val default = OpentelemetryConfig(
      statementInstrumenterEnabled = true,
      statementSanitizationEnabled = true,
      captureQueryParameters = false,
      transactionInstrumenterEnabled = false
    )
  }

}
